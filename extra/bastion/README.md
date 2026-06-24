# BurnBridge Raspberry Pi Web Bastion

This folder contains safe-by-default assets for adding a Caddy + Cockpit web bastion to a Raspberry Pi BurnBridge node.

Default deployment is intentionally staged:

- `install-bastion.sh --prepare-only` installs packages and writes config, but does not enable UFW, disable SSH, or lock root.
- `install-bastion.sh --enable-firewall` enables UFW while keeping SSH allowed from the trusted CIDR.
- `install-bastion.sh --disable-ssh` disables SSH only after Cockpit login has been verified.
- `install-bastion.sh --lock-root` locks root only after deployment automation no longer depends on root SSH.

Always keep an existing SSH session open until Cockpit HTTPS login and BurnBridge S3 smoke tests pass.
