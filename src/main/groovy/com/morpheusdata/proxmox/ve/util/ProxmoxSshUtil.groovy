package com.morpheusdata.proxmox.ve.util

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.LogLevel
import com.morpheusdata.model.TaskResult
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.model.provisioning.WorkloadRequest
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

/**
 * @author Neil van Rensburg
 */

@Slf4j
class ProxmoxSshUtil {

    static String IMAGE_PATH_PREFIX = "/var/opt/morpheus/morpheus-ui/vms/morpheus-images"
    static String REMOTE_IMAGE_DIR = "/var/lib/vz/template/qemu"



    static void createCloudInitDrive(MorpheusContext context, ComputeServer hvNode, WorkloadRequest workloadRequest, String vmId, String datastoreId) {
        log.debug(log.debug("Configuring Cloud-Init"))
        log.debug("Ensuring snippets directory on node: $hvNode.externalId")
        //context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "mkdir -p /var/lib/vz/snippets", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
        runSshCmd(context, hvNode, "mkdir -p /var/lib/vz/snippets")
        log.debug("Creating cloud-init user-data file on hypervisor node: /var/lib/vz/snippets/$vmId-cloud-init-user-data.yml")
        ProxmoxMiscUtil.sftpCreateFile(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "/var/lib/vz/snippets/$vmId-cloud-init-user-data.yml", workloadRequest.cloudConfigUser, null)
        log.debug("Creating cloud-init user-data file on hypervisor node: /var/lib/vz/snippets/$vmId-cloud-init-network.yml")
        ProxmoxMiscUtil.sftpCreateFile(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "/var/lib/vz/snippets/$vmId-cloud-init-network.yml", workloadRequest.cloudConfigNetwork, null)
        log.debug("Creating cloud-init vm disk: $datastoreId:cloudinit")
        //context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "qm set $vmId --ide2 $targetDSs[0].datastore.externalId:cloudinit", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
        runSshCmd(context, hvNode, "qm set $vmId --ide2 $datastoreId:cloudinit")
        log.debug("Mounting cloud-init data to disk...")
        String ciMountCommand = "qm set $vmId --cicustom \"user=local:snippets/$vmId-cloud-init-user-data.yml,network=local:snippets/$vmId-cloud-init-network.yml\""
        //context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, ciMountCommand, "", "", "", false, LogLevel.info, true, null, false).blockingGet()
        runSshCmd(context, hvNode, ciMountCommand)
    }


    public static String uploadImageAndCreateTemplate(MorpheusContext context, HttpApiClient client, Map authConfig, Cloud cloud, VirtualImage virtualImage, ComputeServer hvNode, String targetDS, String imageFile) {
        def imageExternalId
        def lockKey = "proxmox.ve.imageupload.${cloud.regionCode}.${virtualImage?.id}".toString()
        def lock

        try {
            //hold up to a 1 hour lock for image upload
            lock = context.acquireLock(lockKey, [timeout: 2l * 60l * 1000l, ttl: 2l * 60l * 1000l]).blockingGet()

            //create qcow2 template directory on proxmox
            log.debug("Ensuring Image Directory on node: $hvNode.sshHost")
            def dirOut = context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "mkdir -p $REMOTE_IMAGE_DIR", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
            log.debug("Dir create SSH Task \"mkdir -p $REMOTE_IMAGE_DIR\" results: ${dirOut.toMap().toString()}")

            //sftp .qcow2 file to the directory on proxmox server
            log.debug("uploading Image $IMAGE_PATH_PREFIX/$imageFile to $hvNode.sshHost:$REMOTE_IMAGE_DIR")
            ProxmoxMiscUtil.sftpUpload(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "$IMAGE_PATH_PREFIX/$imageFile", REMOTE_IMAGE_DIR, null)

            //create blank vm template on proxmox
            ServiceResponse templateResp = ProxmoxApiComputeUtil.createImageTemplate(client, authConfig, virtualImage.name, hvNode.externalId, 1, 1024L)
            log.debug("Create Image Template response data $templateResp.data")
            imageExternalId = templateResp.data.templateId

            //import the disk file to the blank vm template
            String fileName = new File("$imageFile").getName()
            log.debug("Executing ImportDisk command on node: qm importdisk $imageExternalId $REMOTE_IMAGE_DIR/$fileName $targetDS")
            def diskCreateOut = context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "qm importdisk $imageExternalId $REMOTE_IMAGE_DIR/$fileName $targetDS", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
            log.debug("Disk ImportDisk SSH Task \"qm importdisk $imageExternalId $REMOTE_IMAGE_DIR/$fileName $targetDS\" results: ${diskCreateOut.toMap().toString()}")

            //Mount the disk
            log.debug("Executing DiskMount SSH Task \"qm set $imageExternalId --scsi0 $targetDS:vm-$imageExternalId-disk-0\"")
            def diskMountOut = context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "qm set $imageExternalId --scsi0 $targetDS:vm-$imageExternalId-disk-0", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
            log.debug("Disk Mount SSH Task \"qm set $imageExternalId --scsi0 $targetDS:vm-$imageExternalId-disk-0\" results: ${diskMountOut.toMap().toString()}")
        } finally {
            context.releaseLock(lockKey, [lock:lock]).blockingGet()
        }
        return imageExternalId
    }


    private static runSshCmd(MorpheusContext context, ComputeServer hvNode, String cmd) {
        TaskResult result = context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, cmd, "", "", "", false, LogLevel.info, true, null, false).blockingGet()
        if (!result.success) {
            throw new Exception(result.toMap())
        }
    }

}
