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
        runSshCmd(context, hvNode, "mkdir -p /var/lib/vz/snippets")
        ProxmoxMiscUtil.sftpCreateFile(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "/var/lib/vz/snippets/$vmId-cloud-init-user-data.yml", workloadRequest.cloudConfigUser, null)
        ProxmoxMiscUtil.sftpCreateFile(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "/var/lib/vz/snippets/$vmId-cloud-init-network.yml", workloadRequest.cloudConfigNetwork, null)
        runSshCmd(context, hvNode, "qm set $vmId --ide2 $datastoreId:cloudinit")
        String ciMountCommand = "qm set $vmId --cicustom \"user=local:snippets/$vmId-cloud-init-user-data.yml,network=local:snippets/$vmId-cloud-init-network.yml\""
        runSshCmd(context, hvNode, ciMountCommand)
    }

    /**
     * Upload image file to Proxmox node (does not create template)
     */
    public static String uploadImage(MorpheusContext context, ComputeServer hvNode, String imageFile) {
        log.info("Uploading image $imageFile to node $hvNode.name")

        context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "mkdir -p $REMOTE_IMAGE_DIR", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
        ProxmoxMiscUtil.sftpUpload(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "$IMAGE_PATH_PREFIX/$imageFile", REMOTE_IMAGE_DIR, null)

        String fileName = new File("$imageFile").getName()
        return "$REMOTE_IMAGE_DIR/$fileName"
    }

    /**
     * Create a template from an image file on Proxmox node
     */
    public static String createTemplateFromImage(MorpheusContext context, HttpApiClient client, Map authConfig, VirtualImage virtualImage, ComputeServer hvNode, String targetDS, String remoteImagePath) {
        log.info("Creating template from image on node $hvNode.name, datastore $targetDS")

        ServiceResponse templateResp = ProxmoxApiComputeUtil.createImageTemplate(client, authConfig, virtualImage.name, hvNode.externalId, 1, 1024L)
        def imageExternalId = templateResp.data.templateId

        log.info("Importing disk: qm importdisk $imageExternalId $remoteImagePath $targetDS")
        context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "qm importdisk $imageExternalId $remoteImagePath $targetDS", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
        context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "qm set $imageExternalId --scsi0 $targetDS:vm-$imageExternalId-disk-0 --boot order=scsi0", "", "", "", false, LogLevel.info, true, null, false).blockingGet()

        return imageExternalId
    }

    /**
     * Legacy method - now calls uploadImage and createTemplateFromImage
     * @deprecated Use uploadImage and createTemplateFromImage separately
     */
    public static String uploadImageAndCreateTemplate(MorpheusContext context, HttpApiClient client, Map authConfig, Cloud cloud, VirtualImage virtualImage, ComputeServer hvNode, String targetDS, String imageFile) {
        def imageExternalId
        def lockKey = "proxmox.ve.imageupload.${cloud.regionCode}.${virtualImage?.id}".toString()
        def lock

        try {
            lock = context.acquireLock(lockKey, [timeout: 2l * 60l * 1000l, ttl: 2l * 60l * 1000l]).blockingGet()

            String remoteImagePath = uploadImage(context, hvNode, imageFile)
            imageExternalId = createTemplateFromImage(context, client, authConfig, virtualImage, hvNode, targetDS, remoteImagePath)
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
