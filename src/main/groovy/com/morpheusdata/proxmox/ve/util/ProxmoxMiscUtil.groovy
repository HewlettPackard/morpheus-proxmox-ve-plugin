package com.morpheusdata.proxmox.ve.util

import com.jcraft.jsch.Channel
import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.JSch
import com.jcraft.jsch.Session
import com.morpheusdata.model.MorpheusModel
import groovy.util.logging.Slf4j

/**
 * @author Neil van Rensburg
 */

@Slf4j
class ProxmoxMiscUtil {


    static void sftpUpload(String host, int port, String username, String password, String localPath, String destDir, String privateKeyPath) {
        JSch jsch = new JSch()
        Session session = null
        Channel channel = null
        ChannelSftp channelSftp = null

        try {
            // Set up the private key if provided
            if (privateKeyPath) {
                jsch.addIdentity(privateKeyPath)
            }

            // Open a session to the remote server
            session = jsch.getSession(username, host, port)
            session.setConfig("StrictHostKeyChecking", "no") // Not recommended for production
            session.setPassword(password)
            session.connect()

            // Open the SFTP channel
            channel = session.openChannel("sftp")
            channel.connect()
            channelSftp = channel as ChannelSftp

            // Upload file
            String fileName = new File(localPath).getName()
            String remoteFilePath = destDir.endsWith("/") ? destDir + fileName : destDir + "/" + fileName;
            long startTime = System.currentTimeMillis();
            channelSftp.put(localPath, remoteFilePath)
            long endTime = System.currentTimeMillis()
            long duration = (endTime - startTime) / 1000

        } catch (Exception e) {
            log.error("SFTP FAILED: Error uploading file from $localPath to ${host}:${destDir} - ${e.message}", e)
            throw new Exception("SFTP upload failed: ${e.message}", e)
        } finally {
            if (channelSftp != null) {
                channelSftp.exit()
            }
            if (channel != null) {
                channel.disconnect()
            }
            if (session != null) {
                session.disconnect()
            }
        }
    }


    static void sftpCreateFile(String host, int port, String username, String password, String destFilePath, String content, String privateKeyPath) {
        JSch jsch = new JSch()
        Session session = null
        Channel channel = null
        ChannelSftp channelSftp = null

        try {
            // Set up the private key if provided
            if (privateKeyPath) {
                jsch.addIdentity(privateKeyPath)
            }

            // Open a session to the remote server
            session = jsch.getSession(username, host, port)
            session.setConfig("StrictHostKeyChecking", "no") // Not recommended for production
            session.setPassword(password)
            session.connect()

            // Open the SFTP channel
            channel = session.openChannel("sftp")
            channel.connect()
            channelSftp = channel as ChannelSftp

            // Create and write to the file
            long startTime = System.currentTimeMillis();
            log.debug("SFTP: Creating file ${destFilePath} on ${host} (${content.length()} bytes)")
            ByteArrayInputStream inputStream = new ByteArrayInputStream(content.getBytes("UTF-8"))
            channelSftp.put(inputStream, destFilePath)
            long endTime = System.currentTimeMillis()
            long duration = (endTime - startTime) / 1000

        } catch (Exception e) {
            log.error("SFTP FAILED: Error creating file ${destFilePath} on ${host} - ${e.message}", e)
            throw new Exception("SFTP file creation failed: ${e.message}", e)
        } finally {
            if (channelSftp != null) {
                channelSftp.exit()
            }
            if (channel != null) {
                channel.disconnect()
            }
            if (session != null) {
                session.disconnect()
            }
        }
    }

    static String getNetworkAddress(String ipWithCidr) {
        // Split the input into IP address and prefix length
        def (ip, cidr) = ipWithCidr.tokenize('/')
        int prefixLength = cidr.toInteger()

        // Convert the IP address to a byte array
        byte[] ipBytes = InetAddress.getByName(ip).address

        // Create the subnet mask as a byte array
        byte[] subnetMask = new byte[ipBytes.length]
        for (int i = 0; i < prefixLength; i++) {
            subnetMask[i / 8] |= (1 << (7 - (i % 8)))
        }

        // Calculate the network address by applying the subnet mask
        byte[] networkBytes = new byte[ipBytes.length]
        for (int i = 0; i < ipBytes.length; i++) {
            networkBytes[i] = (byte) (ipBytes[i] & subnetMask[i])
        }

        // Convert the network address back to a string
        String networkAddress = InetAddress.getByAddress(networkBytes).hostAddress

        // Return the network address with the CIDR prefix
        return "$networkAddress/$prefixLength"
    }



    static <T extends MorpheusModel> boolean doUpdateDomainEntity(T model, Map fieldValueMap) {
        boolean changed = false

        fieldValueMap.each { key, newValue ->
            if (model.hasProperty(key)) {
                def currentValue = model."$key"

                // Compare values (null-safe, basic equality check)
                if (currentValue != newValue) {
                    model."$key" = newValue
                    changed = true
                }
            }
        }

        return changed
    }

}
