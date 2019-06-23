import os
indicators = ["alti","tmpf","dwpf","relh","drct","sknt","p01i","alti","mslp","vsby","gust","skyc1","skyc2","skyc3","skyc4","skyl1","skyl2","skyl3","skyl4","wxcodes","feel","ice_accretion_1hr","ice_accretion_3hr",
"ice_accretion_6hr","peak_wind_gust","peak_wind_drct","peak_wind_time"]
for indicator in indicators:
    os.system(f"python3 map.py -i {indicator} -Y 2011 -M 9 -D 1 -ho 1 -m 0 -o {indicator}.png")
