import subprocess
import json
import logging
from typing import Optional, Tuple, Any
import piexif

logger = logging.getLogger(__name__)

class TelemetryHandler:
    def __init__(self):
        self.metadata = {}
        self.has_gps = False

    def extract_metadata(self, video_path: str) -> bool:
        """
        Extracts metadata from the video file using ffmpeg.
        Checks for GPMF or CAMM streams.
        """
        try:
            # Check for streams using ffprobe
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_streams',
                video_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            data = json.loads(result.stdout)
            
            for stream in data.get('streams', []):
                codec_tag_string = stream.get('codec_tag_string', '')
                codec_type = stream.get('codec_type', '')
                
                # Basic check for telemetry streams (GPMF, CAMM)
                if codec_type == 'data':
                    if 'gpmd' in codec_tag_string or 'camm' in codec_tag_string:
                        self.has_gps = True
                        logger.info(f"Found telemetry stream: {codec_tag_string}")
                        # In a real implementation, we would extract the binary data here
                        # using ffmpeg -i video.mp4 -map 0:3 -f data -
                        return True
                        
            logger.info("No known telemetry stream found.")
            return False
            
        except subprocess.CalledProcessError as e:
            logger.error(f"FFprobe error: {e}")
            return False
        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
            return False

    def parse_metadata(self, raw_data: Any) -> None:
        """
        Placeholder for parsing raw telemetry data into a structured format.
        """
        # TODO: Implement GPMF/CAMM parsing logic
        pass

    def get_gps_at_time(self, timestamp: float) -> Optional[Tuple[float, float, float]]:
        """
        Returns (lat, lon, alt) for a given video timestamp (in seconds).
        """
        if not self.has_gps:
            return None
            
        # Placeholder: Return None until parsing logic is implemented
        return None

    def embed_exif(self, image_path: str, lat: float, lon: float, alt: float = 0.0) -> bool:
        """
        Embeds GPS coordinates into the image EXIF data using piexif.
        """
        try:
            # Load existing EXIF or create new
            try:
                exif_dict = piexif.load(image_path)
            except Exception:
                exif_dict = {"0th": {}, "Exif": {}, "GPS": {}, "1st": {}, "thumbnail": None}

            # Helper to convert to rational
            def to_rational(number):
                return (int(number * 1000000), 1000000)

            def to_deg_min_sec(value):
                abs_value = abs(value)
                deg = int(abs_value)
                min_val = (abs_value - deg) * 60
                sec = (min_val - int(min_val)) * 60
                return (to_rational(deg), to_rational(int(min_val)), to_rational(sec))

            lat_deg = to_deg_min_sec(lat)
            lon_deg = to_deg_min_sec(lon)
            
            gps_ifd = {
                piexif.GPSIFD.GPSLatitudeRef: b'N' if lat >= 0 else b'S',
                piexif.GPSIFD.GPSLatitude: lat_deg,
                piexif.GPSIFD.GPSLongitudeRef: b'E' if lon >= 0 else b'W',
                piexif.GPSIFD.GPSLongitude: lon_deg,
                piexif.GPSIFD.GPSAltitudeRef: 0, # Above sea level
                piexif.GPSIFD.GPSAltitude: to_rational(alt)
            }
            
            exif_dict['GPS'] = gps_ifd
            exif_bytes = piexif.dump(exif_dict)
            piexif.insert(exif_bytes, image_path)
            return True
            
        except Exception as e:
            logger.error(f"Error embedding EXIF in {image_path}: {e}")
            return False
