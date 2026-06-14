# Optical Archive Linux Service Scripts

These files install and manage a Linux BurnBridge node that runs both:

- `BurnServer` recorder service
- `versitygw burnbridge` gateway with S3, Admin API, and WebUI

The default layout matches the Raspberry Pi deployment:

- App root: `/opt/burnbridge`
- Shared config: `/opt/burnbridge/config/optical-archive.config.json`
- Runtime data: `/var/lib/burnbridge`
- Logs: `/var/log/burnbridge`
- Optical mount: `/mnt/optical`

## Files

- `optical-archive-start.sh`: manual foreground/background start helper.
- `stop-all.sh`: convenience stop script matching the Windows `stop-all.bat` workflow.
- `optical-archive-service.sh`: install/uninstall/start/stop systemd services.
- `optical-archive-recorder.service`: systemd unit for BurnServer.
- `optical-archive-gateway.service`: systemd unit for versitygw.
- `optical-archive-mount-refresh.service`: optional oneshot mount refresh unit.
- `optical-archive-mount-refresh.sh`: read-only optical mount helper.
- `optical-archive.env.example`: environment file template.

## Manual Start

Copy scripts to the deployed node:

```bash
sudo mkdir -p /opt/burnbridge/scripts /etc/optical-archive
sudo cp optical-archive-start.sh optical-archive-mount-refresh.sh stop-all.sh /opt/burnbridge/scripts/
sudo cp optical-archive.env.example /etc/optical-archive/optical-archive.env
sudo chmod +x /opt/burnbridge/scripts/optical-archive-start.sh /opt/burnbridge/scripts/optical-archive-mount-refresh.sh /opt/burnbridge/scripts/stop-all.sh
```

Start both processes in the background:

```bash
sudo /opt/burnbridge/scripts/optical-archive-start.sh start
```

Other manual commands:

```bash
sudo /opt/burnbridge/scripts/optical-archive-start.sh status
sudo /opt/burnbridge/scripts/optical-archive-start.sh logs
sudo /opt/burnbridge/scripts/optical-archive-start.sh stop
sudo /opt/burnbridge/scripts/stop-all.sh
sudo /opt/burnbridge/scripts/optical-archive-start.sh restart
```

Foreground commands for troubleshooting:

```bash
sudo /opt/burnbridge/scripts/optical-archive-start.sh recorder
sudo /opt/burnbridge/scripts/optical-archive-start.sh gateway
```

## Install Systemd Services

Run from this `extra` directory on the target node:

```bash
sudo ./optical-archive-service.sh install
sudo ./optical-archive-service.sh start
sudo ./optical-archive-service.sh status
```

Manage services:

```bash
sudo ./optical-archive-service.sh stop
sudo ./optical-archive-service.sh restart
sudo ./optical-archive-service.sh logs
sudo ./optical-archive-service.sh uninstall
```

## Configuration

Edit:

```bash
sudo nano /etc/optical-archive/optical-archive.env
```

Common values to adjust:

- `VGW_WEBUI_GATEWAYS`
- `VGW_WEBUI_ADMIN_GATEWAYS`
- `OPTICAL_ARCHIVE_MOUNT_DEVICE`
- `OPTICAL_ARCHIVE_CONFIG_PATH`
- `ROOT_ACCESS_KEY_ID`
- `ROOT_SECRET_ACCESS_KEY`

Notes:

- `OpticalArchive__LinuxServices__MountRefreshServiceType=command` makes BurnServer use the configured command template.
- `OpticalArchive__LinuxServices__MountRefreshCommand` should usually point to `/opt/burnbridge/scripts/optical-archive-mount-refresh.sh "{mountPath}" "{device}"`.
- `MountRefreshServiceType=direct` keeps BurnServer on the built-in `umount` + `mount -o ro` path.
- If the JSON shared config also defines `OpticalArchive:LinuxServices`, restart BurnServer after changing either the JSON config or these environment overrides.
