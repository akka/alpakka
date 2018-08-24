/*
 * Copyright (C) 2008 Wayne Meissner
 *
 * This file is part of the JNR project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// FIXME: This file can be removed once https://github.com/jnr/jnr-enxio/pull/28 is in.

package jnr.enxio.channels;

import jnr.constants.platform.Errno;
import jnr.ffi.Memory;
import jnr.ffi.Pointer;
import jnr.ffi.StructLayout;
import jnr.ffi.TypeAlias;
import jnr.ffi.provider.jffi.NativeRuntime;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of a {@link java.nio.channels.Selector} that uses the BSD (including MacOS)
 * kqueue(2) mechanism
 */
class KQSelector extends java.nio.channels.spi.AbstractSelector {
  private static final boolean DEBUG = false;
  private static final int MAX_EVENTS = 100;
  private static final int EVFILT_READ = -1;
  private static final int EVFILT_WRITE = -2;
  private static final int EV_ADD = 0x0001;
  private static final int EV_DELETE = 0x0002;
  private static final int EV_ENABLE = 0x0004;
  private static final int EV_DISABLE = 0x0008;
  private static final int EV_CLEAR = 0x0020;

  private int kqfd = -1;
  private final jnr.ffi.Runtime runtime = NativeRuntime.getSystemRuntime();
  private final Pointer changebuf;
  private final Pointer eventbuf;
  private final EventIO io = EventIO.getInstance();
  private final int[] pipefd = {-1, -1};
  private final Object regLock = new Object();
  private final Map<Integer, Descriptor> descriptors = new ConcurrentHashMap<Integer, Descriptor>();
  private final Set<SelectionKey> selected = new LinkedHashSet<SelectionKey>();
  private final Native.Timespec ZERO_TIMESPEC = new Native.Timespec(0, 0);

  public KQSelector(NativeSelectorProvider provider) {
    super(provider);
    changebuf = Memory.allocateDirect(runtime, MAX_EVENTS * io.size());
    eventbuf = Memory.allocateDirect(runtime, MAX_EVENTS * io.size());

    Native.libc().pipe(pipefd);

    kqfd = Native.libc().kqueue();
    io.put(changebuf, 0, pipefd[0], EVFILT_READ, EV_ADD);
    Native.libc().kevent(kqfd, changebuf, 1, null, 0, ZERO_TIMESPEC);
  }

  private static class Descriptor {
    private final int fd;
    private final Set<KQSelectionKey> keys = new HashSet<KQSelectionKey>();
    private boolean write = false, read = false;

    public Descriptor(int fd) {
      this.fd = fd;
    }
  }

  @Override
  protected void implCloseSelector() throws IOException {
    if (kqfd != -1) {
      Native.close(kqfd);
    }
    if (pipefd[0] != -1) {
      Native.close(pipefd[0]);
    }
    if (pipefd[1] != -1) {
      Native.close(pipefd[1]);
    }
    pipefd[0] = pipefd[1] = kqfd = -1;

    // deregister all keys
    for (Map.Entry<Integer, Descriptor> entry : descriptors.entrySet()) {
      for (KQSelectionKey k : entry.getValue().keys) {
        deregister(k);
      }
    }
  }

  @Override
  protected SelectionKey register(AbstractSelectableChannel ch, int ops, Object att) {
    KQSelectionKey k = new KQSelectionKey(this, (NativeSelectableChannel) ch, ops);
    synchronized (regLock) {
      Descriptor d = new Descriptor(k.getFD());
      descriptors.put(k.getFD(), d);
      d.keys.add(k);
      handleChangedKey(d);
    }
    k.attach(att);
    return k;
  }

  @Override
  public Set<SelectionKey> keys() {
    Set<SelectionKey> keys = new HashSet<SelectionKey>();
    for (Descriptor fd : descriptors.values()) {
      keys.addAll(fd.keys);
    }
    return Collections.unmodifiableSet(keys);
  }

  @Override
  public Set<SelectionKey> selectedKeys() {
    return selected;
  }

  @Override
  public int selectNow() throws IOException {
    return poll(0);
  }

  @Override
  public int select(long timeout) throws IOException {
    return poll(timeout);
  }

  @Override
  public int select() throws IOException {
    return poll(-1);
  }

  private int poll(long timeout) {

    int nchanged = handleCancelledKeys();

    Native.Timespec ts = null;
    if (timeout >= 0) {
      long sec = TimeUnit.MILLISECONDS.toSeconds(timeout);
      long nsec = TimeUnit.MILLISECONDS.toNanos(timeout % 1000);
      ts = new Native.Timespec(sec, nsec);
    }

    if (DEBUG) System.out.printf("nchanged=%d\n", nchanged);
    int nready = 0;
    try {
      begin();
      do {

        nready = Native.libc().kevent(kqfd, changebuf, nchanged, eventbuf, MAX_EVENTS, ts);

      } while (nready < 0 && Errno.EINTR.equals(Errno.valueOf(Native.getRuntime().getLastError())));

      if (DEBUG) System.out.println("kevent returned " + nready + " events ready");

    } finally {
      end();
    }

    int updatedKeyCount = 0;
    synchronized (regLock) {
      for (int i = 0; i < nready; ++i) {
        int fd = io.getFD(eventbuf, i);
        Descriptor d = descriptors.get(fd);

        if (d != null) {
          int filt = io.getFilter(eventbuf, i);
          if (DEBUG) System.out.printf("fd=%d filt=0x%x\n", d.fd, filt);
          for (KQSelectionKey k : d.keys) {
            int iops = k.interestOps();
            int ops = 0;

            if (filt == EVFILT_READ) {
              ops |= iops & (SelectionKey.OP_ACCEPT | SelectionKey.OP_READ);
            }
            if (filt == EVFILT_WRITE) {
              ops |= iops & (SelectionKey.OP_CONNECT | SelectionKey.OP_WRITE);
            }
            ++updatedKeyCount;
            k.readyOps(ops);
            if (!selected.contains(k)) {
              selected.add(k);
            }
          }

        } else if (fd == pipefd[0]) {
          if (DEBUG) System.out.println("Waking up");
          wakeupReceived();
        }
      }
    }
    return updatedKeyCount;
  }

  private int handleCancelledKeys() {
    Set<SelectionKey> cancelled = cancelledKeys();
    synchronized (cancelled) {
      int nchanged = 0;
      synchronized (regLock) {
        for (SelectionKey k : cancelled) {
          KQSelectionKey kqs = (KQSelectionKey) k;
          Descriptor d = descriptors.get(kqs.getFD());
          deregister(kqs);
          synchronized (selected) {
            selected.remove(kqs);
          }
          d.keys.remove(kqs);
          if (d.keys.isEmpty()) {
            io.put(changebuf, nchanged++, kqs.getFD(), EVFILT_READ, EV_DELETE);
            io.put(changebuf, nchanged++, kqs.getFD(), EVFILT_WRITE, EV_DELETE);
            descriptors.remove(kqs.getFD());
          }
          if (nchanged >= MAX_EVENTS) {
            Native.libc().kevent(kqfd, changebuf, nchanged, null, 0, ZERO_TIMESPEC);
            nchanged = 0;
          }
        }
      }
      cancelled.clear();
      return nchanged;
    }
  }

  private void handleChangedKey(Descriptor changed) {
    synchronized (regLock) {
      int _nchanged = 0;
      int writers = 0, readers = 0;
      for (KQSelectionKey k : changed.keys) {
        if ((k.interestOps() & (SelectionKey.OP_ACCEPT | SelectionKey.OP_READ)) != 0) {
          ++readers;
        }
        if ((k.interestOps() & (SelectionKey.OP_CONNECT | SelectionKey.OP_WRITE)) != 0) {
          ++writers;
        }
      }
      for (Integer filt : new Integer[] {EVFILT_READ, EVFILT_WRITE}) {
        int flags = 0;
        //
        // If no one is interested in events on the fd, disable it
        //
        if (filt == EVFILT_READ) {
          if (readers > 0 && !changed.read) {
            flags = EV_ADD | EV_ENABLE | EV_CLEAR;
            changed.read = true;
          } else if (readers == 0 && changed.read) {
            flags = EV_DISABLE;
            changed.read = false;
          }
        }
        if (filt == EVFILT_WRITE) {
          if (writers > 0 && !changed.write) {
            flags = EV_ADD | EV_ENABLE | EV_CLEAR;
            changed.write = true;
          } else if (writers == 0 && changed.write) {
            flags = EV_DISABLE;
            changed.write = false;
          }
        }
        if (DEBUG)
          System.out.printf("Updating fd %d filt=0x%x flags=0x%x\n", changed.fd, filt, flags);
        if (flags != 0) {
          io.put(changebuf, _nchanged++, changed.fd, filt, flags);
        }
      }

      Native.libc().kevent(kqfd, changebuf, _nchanged, null, 0, ZERO_TIMESPEC);
    }
  }

  private void wakeupReceived() {
    Native.libc().read(pipefd[0], new byte[1], 1);
  }

  @Override
  public Selector wakeup() {
    Native.libc().write(pipefd[1], new byte[1], 1);
    return this;
  }

  void interestOps(KQSelectionKey k, int ops) {
    synchronized (regLock) {
      handleChangedKey(descriptors.get(k.getFD()));
    }
  }

  private static final class EventIO {
    private static final EventIO INSTANCE = new EventIO();
    private final EventLayout layout = new EventLayout(NativeRuntime.getSystemRuntime());
    private final jnr.ffi.Type uintptr_t = layout.getRuntime().findType(TypeAlias.uintptr_t);

    public static EventIO getInstance() {
      return EventIO.INSTANCE;
    }

    public final void put(Pointer buf, int index, int fd, int filt, int flags) {
      buf.putInt(uintptr_t, (index * layout.size()) + layout.ident.offset(), fd);
      buf.putShort((index * layout.size()) + layout.filter.offset(), (short) filt);
      buf.putInt((index * layout.size()) + layout.flags.offset(), flags);
    }

    public final int size() {
      return layout.size();
    }

    int getFD(Pointer ptr, int index) {
      return (int) ptr.getInt(uintptr_t, (index * layout.size()) + layout.ident.offset());
    }

    public final void putFilter(Pointer buf, int index, int filter) {
      buf.putShort((index * layout.size()) + layout.filter.offset(), (short) filter);
    }

    public final int getFilter(Pointer buf, int index) {
      return buf.getShort((index * layout.size()) + layout.filter.offset());
    }

    public final void putFlags(Pointer buf, int index, int flags) {
      buf.putShort((index * layout.size()) + layout.flags.offset(), (short) flags);
    }
  }

  private static class EventLayout extends StructLayout {
    private EventLayout(jnr.ffi.Runtime runtime) {
      super(runtime);
    }

    public final uintptr_t ident = new uintptr_t();
    public final int16_t filter = new int16_t();
    public final u_int16_t flags = new u_int16_t();
    public final u_int32_t fflags = new u_int32_t();
    public final intptr_t data = new intptr_t();
    public final Pointer udata = new Pointer();
  }
}
