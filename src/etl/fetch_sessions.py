import fastf1
import pandas as pd
from pathlib import Path

def process_session(year, grand_prix, session_type='R'):
    # Enable cache folder
    cache_path = Path("cache")
    cache_path.mkdir(parents=True, exist_ok=True)
    fastf1.Cache.enable_cache(str(cache_path))

    session = fastf1.get_session(year, grand_prix, session_type)
    session.load(telemetry=True, laps=True, weather=True)

    race_slug = f"{year}_{grand_prix.replace(' ', '_')}"
    raw_path = Path(f"data/raw/{race_slug}")
    processed_path = Path(f"data/processed/{race_slug}")

    raw_path.mkdir(parents=True, exist_ok=True)
    processed_path.mkdir(parents=True, exist_ok=True)

    # Save raw laps if not exists
    raw_laps_file = raw_path / "laps.csv"
    if not raw_laps_file.exists():
        session.laps.to_csv(raw_laps_file, index=False)

    # Handle session start time safely
    start_time = session.session_start_time
    is_datetime = hasattr(start_time, "timestamp")

    for drv in session.drivers:
        driver_laps = session.laps.pick_drivers([drv])  # use pick_drivers (plural)

        telemetry_dfs = []
        for _, lap in driver_laps.iterlaps():
            lap_data = lap.get_car_data().add_distance()
            lap_data['Driver'] = drv
            lap_data['LapNumber'] = lap.LapNumber
            lap_data['Stint'] = lap.Stint
            lap_data['TyreCompound'] = lap.Compound
            telemetry_dfs.append(lap_data)

        full_telemetry = pd.concat(telemetry_dfs, ignore_index=True)

        if is_datetime:
            epoch_start = start_time.timestamp()
            full_telemetry['TimeSeconds'] = full_telemetry['Time'].apply(lambda x: x.timestamp() - epoch_start)
        else:
            full_telemetry['TimeSeconds'] = full_telemetry['Time'].dt.total_seconds()

        full_telemetry.rename(columns={
            'Speed': 'Speed_kph',
            'Throttle': 'Throttle_pct',
            'Brake': 'Brake_pct',
            'Gear': 'Gear_num',
            'Distance': 'Distance_m'
        }, inplace=True)

        processed_file = processed_path / f"telemetry_{drv}.csv.gz"
        full_telemetry.to_csv(processed_file, index=False, compression='gzip')
        print(f"Saved processed telemetry for driver {drv} at {processed_file}")

    laps_df = session.laps.copy()

    if 'LapTime' not in laps_df.columns or laps_df['LapTime'].isnull().all():
        laps_df['LapTime'] = laps_df['LapTime'].dt.total_seconds()

    processed_laps_file = processed_path / "laps.csv.gz"
    laps_df.to_csv(processed_laps_file, index=False, compression='gzip')
    print(f"Saved processed laps data at {processed_laps_file}")

if __name__ == "__main__":
    process_session(2025, 'Hungarian Grand Prix', 'R')
