/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp.examples;

//#create-settings
import akka.stream.alpakka.ftp.FtpCredentials;
import akka.stream.alpakka.ftp.FtpSettings;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class FtpSettingsExample {
    private FtpSettings settings;

    public FtpSettingsExample() {
        try {
            settings =  new FtpSettings(
                    InetAddress.getByName("localhost"),
                    FtpSettings.DefaultFtpPort(),
                    FtpCredentials.createAnonCredentials(),
                    false, // binary
                    true // passiveMode
            );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
//#create-settings