# CLI Testing Protocol

This protocol provides a structured guide for testing the Command Line Interface (CLI) of Application360, focusing on robustness for headless deployments.

## 1. Objective
To verify that the CLI functions correctly without a graphical user interface (GUI), ensuring reliability for automated batch processing and server-side extraction.

## 2. Prerequisites
Before beginning the testing protocol, ensure the following requirements are met:
- **Python Environment:** Python 3.8+ installed.
- **Dependencies:** Install required libraries:
  ```bash
  pip install requests tqdm
  ```
- **Sample Video:** A sample 360 video file (e.g., `videos/sample_360.mp4`).

---

## 3. Test Cases

### Case 1: Basic Headless Extraction
**Goal:** Verify the extraction process runs successfully without launching the GUI.

| Action | Command |
| :--- | :--- |
| **Run Extraction** | `python src/main.py --input videos/sample_360.mp4 --output tests/output/basic --interval 1.0` |

**Expected Result:**
- No GUI window appears.
- Console shows a progress bar (if `tqdm` is installed).
- Output directory `tests/output/basic` is created and contains extracted images.
- Logs indicate successful completion.

### Case 2: Selective Camera Direction
**Goal:** Verify the `--active-cameras` flag correctly limits extraction to specific views.

| Action | Command |
| :--- | :--- |
| **Select Cameras** | `python src/main.py --input videos/sample_360.mp4 --output tests/output/selective --active-cameras "0,2"` |

**Expected Result:**
- Only the Front (`_Front.jpg`) and Back (`_Back.jpg`) views are generated for each frame.
- No files for Right, Left, Up, or Down views are present in the output directory.

### Case 3: Telemetry Export
**Goal:** Verify the `--export-telemetry` flag embeds GPS metadata into output images.

| Action | Command |
| :--- | :--- |
| **Export Telemetry** | `python src/main.py --input videos/sample_360.mp4 --output tests/output/telemetry --export-telemetry` |

**Expected Result:**
- Output images are generated.
- Extracted images contain EXIF GPS tags (Latitude, Longitude, Altitude).
- Verification via `exiftool` or file properties confirms valid coordinates.

### Case 4: Adaptive Interval
**Goal:** Verify the `--adaptive` flag correctly skips static frames based on motion detection.

| Action | Command |
| :--- | :--- |
| **Run Adaptive** | `python src/main.py --input videos/sample_360.mp4 --output tests/output/adaptive --adaptive --motion-threshold 10.0` |

**Expected Result:**
- The number of generated frames is lower than a fixed-interval extraction (if the video contains static segments).
- Console logs reflect frames being skipped due to low motion.

### Case 5: Layout Modes
**Goal:** Verify that different projection layouts (`cube`, `fibonacci`) are correctly applied.

| Action | Command |
| :--- | :--- |
| **Test Cube Layout** | `python src/main.py --input videos/sample_360.mp4 --output tests/output/cube --layout cube` |
| **Test Fibonacci** | `python src/main.py --input videos/sample_360.mp4 --output tests/output/fib --layout fibonacci --camera-count 20` |

**Expected Result:**
- **Cube:** 6 images per frame with standard cube-map naming (Front, Back, etc.).
- **Fibonacci:** 20 images per frame distributed evenly across the sphere.

---

## 4. Troubleshooting
- **Missing Progress Bar:** Ensure `tqdm` is installed (`pip install tqdm`).
- **Input Error:** Verify the video path is correct and accessible.
- **Permissions:** Ensure the application has write permissions for the output directory.
