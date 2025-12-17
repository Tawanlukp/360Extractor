# Testing the CLI for Application360

This document explains how to use the new Command Line Interface (CLI) for Application360. This feature allows for headless extraction (e.g., on a cloud server without a GUI) and selective camera processing.

## Introduction

The tool now supports a CLI mode that bypasses the graphical interface when specific arguments are provided. This is ideal for batch processing, automation, or running the tool in environments where a display is not available.

## Prerequisites

Ensure all dependencies are installed. For the progress bar to function correctly in the CLI, `tqdm` is recommended:

```bash
pip install tqdm
```

## Section 1: Basic Headless Extraction

To extract frames from a 360 video without opening the GUI, use the `--input` and `--output` flags.

**Command Syntax:**
```bash
python src/main.py --input <path_to_video> --output <output_folder> [options]
```

**Example:**
```bash
python src/main.py --input videos/sample_360.mp4 --output extracted_frames --interval 1.0
```

**What to Expect:**
*   The application will start in CLI mode.
*   Logs will be printed to the console indicating progress.
*   If `tqdm` is installed, a progress bar will show the extraction status.
*   Frames will be saved in the specified output directory.

## Section 2: Selective Camera Extraction

You can limit extraction to specific virtual camera views (e.g., only Front and Back) using the `--active-cameras` flag.

**Camera Indices (Default 6-camera setup):**
*   0: Front
*   1: Right
*   2: Back
*   3: Left
*   4: Up
*   5: Down

**Command Syntax:**
```bash
python src/main.py ... --active-cameras "index1,index2,..."
```

**Example:**
To extract only the Front (0) and Back (2) views:
```bash
python src/main.py --input videos/sample_360.mp4 --output extracted_frames --active-cameras "0,2"
```

**Result:**
Only files ending in `_Front.jpg` and `_Back.jpg` will be generated.

## Section 3: Using a Configuration File

For complex jobs, you can define all settings in a JSON configuration file and pass it via the `--config` flag.

**Sample Configuration (`job_config.json`):**
```json
{
    "input": "videos/sample_360.mp4",
    "output": "output_folder",
    "interval": 2.0,
    "format": "jpg",
    "active_cameras": [0, 2, 4],
    "quality": 90,
    "ai": true
}
```

**Command Syntax:**
```bash
python src/main.py --config job_config.json
```

**Note:** Command-line arguments override configuration file settings. For example, if your config specifies `interval: 2.0` but you run with `--interval 0.5`, the CLI argument (0.5) will be used.

## Section 4: Verification Scripts

We have provided test scripts to quickly verify these features work as expected.

1.  **Generate a Dummy Video (if needed):**
    `tests/create_dummy_video.py` can create a small test video file for experimentation.
    ```bash
    python tests/create_dummy_video.py tests/test_video.mp4
    ```

2.  **Verify Selective Cameras:**
    Run the automated verification script to ensure only requested cameras are extracted.
    ```bash
    python tests/verify_selective_cameras.py
    ```
    *This script creates a temporary video, runs the CLI requesting cameras 0 and 2, and asserts that only those files were created.*

3.  **Verify Layout Logic:**
    Run the geometry verification script to check if the layout logic (Adaptive vs Ring) produces the correct camera names.
    ```bash
    python tests/verify_layout_toggle.py
    ```

## Section 5: Testing Layout Toggle

You can now force a specific camera layout using the `--layout` argument. This is particularly useful if you want a 6-camera ring layout instead of the default cube layout.

**1. Force Ring Layout (6 Cameras):**
This command forces a horizontal ring layout even for 6 cameras (which usually defaults to Cube).
```bash
python src/main.py --input videos/sample_360.mp4 --output frames/ring_test --camera-count 6 --layout ring
```
*Result: 6 horizon views (View_0 to View_5).*

**2. Adaptive Layout (Default):**
This uses the standard behavior (Cube for 6 cameras).
```bash
python src/main.py --input videos/sample_360.mp4 --output frames/cube_test --camera-count 6 --layout adaptive
```
*Result: Cube views (Front, Right, Back, Left, Up, Down).*

## Section 6: Testing Adaptive Interval (Intelligent Keyframing)

You can test the motion-based extraction which skips static scenes.

**1. Basic Adaptive Extraction:**
Enable adaptive mode with the default threshold.
```bash
python src/main.py --input videos/sample_360.mp4 --output frames/adaptive_test --adaptive
```

**2. Custom Motion Threshold:**
Adjust the sensitivity. Higher values (e.g., 20.0) require more motion to trigger extraction.
```bash
python src/main.py --input videos/sample_360.mp4 --output frames/adaptive_high_thresh --adaptive --motion-threshold 20.0
```

**3. Verification Script:**
We have provided a script that creates a synthetic video with static and moving segments to verify that frames are skipped correctly.
```bash
python tests/verify_adaptive.py
```
*This script runs the CLI with `--adaptive` on a test video and asserts that the number of extracted frames matches the expected count (skipping static parts).*

## Section 7: Testing Telemetry Export

You can verify the GPS/IMU metadata integration. This feature extracts telemetry streams from the video and embeds coordinates into the output images.

**1. Basic Telemetry Extraction:**
Run the CLI with the `--export-telemetry` flag.
```bash
python src/main.py --input videos/sample_360.mp4 --output frames/telemetry_test --export-telemetry
```
*Result: Output images will contain EXIF GPS tags (latitude, longitude, altitude) derived from the video telemetry.*

**2. Verification:**
You can inspect the EXIF data of the generated images using tools like `exiftool` or by checking file properties.