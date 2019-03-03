/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.ssl.SslConfigurationFactory;

import java.io.File;

abstract class FtpsSupportImpl extends PlainFtpSupportImpl {

  static final String AUTH_VALUE_SSL = "SSL";
  static final String AUTH_VALUE_TLS = "TLS";
  private static final String FTP_SERVER_KEY_STORE_PASSWORD = "password";

  private String clientAuth = "none";
  private String authValue;
  private Boolean useImplicit;

  private File ftpServerKeyStore;

  protected FtpsSupportImpl() {
    ftpServerKeyStore = new File(getClass().getClassLoader().getResource("server.jks").getFile());
  }

  @Override
  protected FtpServerFactory createFtpServerFactory(Integer port) {
    FtpServerFactory factory = super.createFtpServerFactory(port);
    ListenerFactory listenerFactory = new ListenerFactory(factory.getListener(DEFAULT_LISTENER));
    listenerFactory.setImplicitSsl(getUseImplicit());
    listenerFactory.setSslConfiguration(sslConfigFactory().createSslConfiguration());
    factory.addListener(DEFAULT_LISTENER, listenerFactory.createListener());
    return factory;
  }

  private SslConfigurationFactory sslConfigFactory() {
    SslConfigurationFactory factory = new SslConfigurationFactory();
    factory.setSslProtocol(getAuthValue());

    factory.setKeystoreFile(ftpServerKeyStore);
    factory.setKeystoreType("JKS");
    factory.setKeystoreAlgorithm("SunX509");
    factory.setKeyPassword(FTP_SERVER_KEY_STORE_PASSWORD);
    factory.setKeystorePassword(FTP_SERVER_KEY_STORE_PASSWORD);

    factory.setClientAuthentication(getClientAuth());

    factory.setTruststoreFile(ftpServerKeyStore);
    factory.setTruststoreType("JKS");
    factory.setTruststoreAlgorithm("SunX509");
    factory.setTruststorePassword(FTP_SERVER_KEY_STORE_PASSWORD);

    return factory;
  }

  String getClientAuth() {
    return clientAuth;
  }

  void setClientAuth(String clientAuth) {
    this.clientAuth = clientAuth;
  }

  String getAuthValue() {
    return authValue;
  }

  void setAuthValue(String authValue) {
    this.authValue = authValue;
  }

  Boolean getUseImplicit() {
    return useImplicit;
  }

  void setUseImplicit(Boolean useImplicit) {
    this.useImplicit = useImplicit;
  }
}
