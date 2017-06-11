/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.hbase.Utils;

import org.apache.commons.lang3.reflect.FieldUtils;
import sun.net.spi.nameservice.NameService;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;


/**
 * Fake "foo" DNS resolution
 */
@SuppressWarnings("restriction")
public class DNSUtils implements sun.net.spi.nameservice.NameService {
    private static final String HBASE = "127.0.0.1";
    private String hostName;

    public DNSUtils(String hbaseHostName) {
        hostName = hbaseHostName;
    }

    @Override
    public InetAddress[] lookupAllHostAddr(String paramString) throws UnknownHostException {
        if (hostName.equals(paramString)) {
            final byte[] arrayOfByte = sun.net.util.IPAddressUtil.textToNumericFormatV4(HBASE);
            final InetAddress address = InetAddress.getByAddress(paramString, arrayOfByte);
            return new InetAddress[]{address};
        } else {
            throw new UnknownHostException();
        }
    }

    @Override
    public String getHostByAddr(byte[] paramArrayOfByte) throws UnknownHostException {
        throw new UnknownHostException();
    }

    public static void setupDNS(String hostName) {
        try {
            @SuppressWarnings("unchecked") List<NameService> nameServices =
                    (List<sun.net.spi.nameservice.NameService>) FieldUtils.readStaticField(InetAddress.class, "nameServices", true);
            nameServices.add(new DNSUtils(hostName));
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
