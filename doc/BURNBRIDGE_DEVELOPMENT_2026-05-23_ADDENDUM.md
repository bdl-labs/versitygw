# BurnBridge Development Addendum 2026-05-23

## Summary

- BurnServer startup probing now resolves a stable synthetic disc serial and volume label when media exposes no native disc identity.
- Shared archive config now includes recorder identity strategy fields and exposes them through `GET` / `PUT /__archive/config` and the WebUI.
- BurnBridge now implements `CreateBucket` as the initial blank-disc naming operation.
- Rebinding to a new bucket is blocked once the current media already has committed objects.
- Probe-volume to logical-bucket binding is persisted into shared config for restart safety.
