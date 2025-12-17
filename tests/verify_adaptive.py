import cv2
import numpy as np
import os
import shutil
import sys

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from core.job import Job
from core.processor import ProcessingWorker
from core.motion_detector import MotionDetector

def create_dummy_video(filename, width=640, height=360, fps=30, duration=2):
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(filename, fourcc, fps, (width, height))
    
    # First 1 second: Static (Stationary Rectangle)
    frame_static = np.zeros((height, width, 3), dtype=np.uint8)
    cv2.rectangle(frame_static, (100, 100), (200, 200), (255, 255, 255), -1)
    
    for _ in range(fps):
        out.write(frame_static)
        
    # Next 1 second: Motion (Moving Rectangle)
    for i in range(fps):
        frame = np.zeros((height, width, 3), dtype=np.uint8)
        # Move by 20 pixels each frame, starting from offset 20 to ensure difference
        offset = (i + 1) * 20
        cv2.rectangle(frame, (100 + offset, 100), (200 + offset, 200), (255, 255, 255), -1)
        out.write(frame)
            
    out.release()
    print(f"Created dummy video: {filename}")

def test_motion_detector():
    print("Testing MotionDetector...")
    md = MotionDetector()
    
    # Static frames
    f1 = np.zeros((144, 256, 3), dtype=np.uint8)
    cv2.rectangle(f1, (50, 50), (100, 100), (255, 255, 255), -1)
    
    f2 = f1.copy() # Identical
    
    score_static = md.calculate_motion_score(f1, f2)
    print(f"Static Score: {score_static} (Expected ~0)")
    
    # Motion frame (rectangle moved)
    f3 = np.zeros((144, 256, 3), dtype=np.uint8)
    cv2.rectangle(f3, (60, 60), (110, 110), (255, 255, 255), -1)
    
    score_motion = md.calculate_motion_score(f1, f3)
    print(f"Motion Score: {score_motion} (Expected > 0)")
    
    assert score_static < 0.1
    assert score_motion > 0.1
    print("MotionDetector Test Passed.")

def run_adaptive_test():
    print("\nTesting Adaptive Extraction...")
    video_path = "test_adaptive_video.mp4"
    create_dummy_video(video_path)
    
    output_dir = "test_adaptive_output"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)
    
    # Job Settings
    settings = {
        'adaptive_mode': True,
        'adaptive_threshold': 0.5,
        'interval_value': 1.0, # Check every frame
        'interval_unit': 'Frames',
        'custom_output_dir': output_dir,
        'camera_count': 6,
        'resolution': 256,
        'fov': 90,
        'layout_mode': 'adaptive'
    }
    
    job = Job(file_path=video_path, settings=settings)
    
    # Create worker
    worker = ProcessingWorker([job])
    
    # Connect signals to print output
    worker.progress_updated.connect(lambda v, m: print(f"Progress: {v}% - {m}"))
    worker.error_occurred.connect(lambda e: print(f"ERROR: {e}"))
    
    print("Running processor...")
    worker.run()
    
    # Count output files
    processed_dir = os.path.join(output_dir, "test_adaptive_video_processed")
    if not os.path.exists(processed_dir):
         print("FAILURE: Output directory not created.")
         return

    files = os.listdir(processed_dir)
    jpg_files = [f for f in files if f.endswith('.jpg')]
    
    count = len(jpg_files)
    print(f"Generated {count} frames.")
    
    # Analysis:
    # First 30 frames static: Frame 0 saved (6). Frame 1-29 skipped.
    # Next 30 frames motion: All saved (30*6 = 180).
    # Expected ~ 186.
    
    if 180 <= count <= 200:
        print("SUCCESS: Frame count is within expected range for adaptive mode.")
    elif count == 360:
        print("FAILURE: All frames were saved. Adaptive mode failed.")
    else:
        print(f"WARNING: unexpected frame count {count}. Expected ~186.")

    # Cleanup
    if os.path.exists(video_path):
        os.remove(video_path)
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

if __name__ == "__main__":
    test_motion_detector()
    run_adaptive_test()
