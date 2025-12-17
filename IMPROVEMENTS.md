# 360 Extractor Improvements & Todo List

This document tracks planned improvements and known issues for 360 Extractor.

## High Priority

- [ ] **Smart Masking (NeRF/Gaussian Splatting Prep)**
    - **Context:** Training NeRFs or Gaussian Splats requires clean static scenes. Moving objects (people, cars, leaves) cause artifacts ("floaters" or blurring). Current removal leaves holes; masking is preferred by advanced pipelines.
    - **Goal:** Elevate the AI module to export precise binary masks alongside images.
        - Option to mask *all* dynamic objects (classes: person, car, bicycle, etc.).
        - Ensure mask format is compatible with RealityCapture/Nerfstudio (e.g., Black=Subject, White=Background).
    - **Impact:** Significantly cleaner high-end 3D reconstructions with less manual cleanup work.

## Professional Future Ideas

- [ ] **Generative AI Inpainting:** Background reconstruction behind the operator/dynamic objects for "clean-plate" datasets.
- [ ] **IMU-Based Auto-Horizon Leveling:** Using gyro/accel data to perfectly level every extracted frame.
- [ ] **Project-Based Session Management:** `.360proj` files to save settings and resume heavy batch processing.
- [ ] **Native Dual-Fisheye Support:** Parse/stitch raw `.insv` and `.360` files directly.
- [ ] **Software-Specific Export Profiles:** One-click bundles for RealityCapture, Nerfstudio, and Mapillary.
- [ ] **Depth-Aware Masking:** Monocular depth estimation for geometric priors in Gaussian Splatting.
- [ ] **Cloud Processing Bridge:** Seamlessly offload heavy extraction to headless GPU cloud instances via CLI.

# Completed

- [x] **CLI for Headless/Cloud & Progress Bar**
    - Implemented `argparse` in `src/main.py` to allow running extraction jobs without opening the GUI.
    - Integrated `tqdm` visual progress bar for real-time status tracking in terminal.
- [x] **Selective Camera Direction**
    - Added UI/CLI mechanism to select active cameras from generated views.
- [x] **Robust Logging & Config Files**
    - Replaced `print` statements with Python's `logging` module.
    - Configuration File Support (JSON/YAML) to allow loading settings from a file.
- [x] **Intelligent Keyframing (Optical Flow)**
    - Adaptive Interval mode using optical flow analysis to only extract frames with significant scene change.
- [x] **GPS/IMU Metadata Integration (GoPro/Insta360/DJI)**
    - Extract telemetry data (GPMF/CAMM) from source videos and embed pose/location data into EXIF or sidecar files.
