import { stationCoordinates } from "../../config/gtfs/station-coordinates";
import { DatabaseConnection } from "../database/DatabaseConnection";
import { CLICommand } from "./CLICommand";
import * as fs from "fs";
import * as path from "path";

interface StationInfo {
  crs_code: string;
  tiploc_code: string;
  station_name: string;
  cate_interchange_status: number | null;
  easting: number | null;
  northing: number | null;
}

export class ConsistencyReportCommand implements CLICommand {
  private baseDir: string = "./consistency-out";

  public constructor(private readonly db: DatabaseConnection) {}

  public async run(_argv: string[]): Promise<void> {
    console.log("Creating consistency report");

    try {
      fs.rmSync(this.baseDir, { recursive: true });
    } catch (err) {}
    fs.mkdirSync(this.baseDir, { recursive: true });

    this.writeRecords(
      "stations_not_in_coordinates_file.ndjson",
      await this.getStationsNotInCoordinatesFile()
    );

    this.writeRecords(
      "stations_outside_uk.ndjson",
      await this.getStationsOutsideUK()
    );

    await this.db.end();

    console.log("Wrote consistency report to", this.baseDir);
  }

  writeRecords(filename: string, records: Record<string, any>[]) {
    const filePath = path.join(this.baseDir, filename);
    const f = fs.openSync(filePath, "w+");
    const enc = new TextEncoder();
    for (const record of records) {
      fs.writeSync(f, enc.encode(JSON.stringify(record) + "\n"));
    }
    fs.closeSync(f);
    console.log("Wrote", filePath);
  }

  async getStationsOutsideUK(): Promise<StationInfo[]> {
    const [results] = await this.db.query<StationInfo>(`
      SELECT *
      FROM (SELECT crs_code,
                  tiploc_code,
                  station_name,
                  cate_interchange_status,
                  (CAST(easting AS double) - 10000) * 100  as easting,
                  (CAST(northing AS double) - 60000) * 100 as northing
            FROM physical_station
            WHERE crs_code IS NOT NULL
            GROUP BY crs_code) as stations
      WHERE
          easting IS NULL or northing IS NULL
          OR easting < -69576 OR northing < 6627
          OR easting > 639158 OR northing > 1009282
      `);
    return results;
  }

  async getStationsNotInCoordinatesFile(): Promise<StationInfo[]> {
    const [dbList] = await this.db.query<{ crs_code: string }>(
      `SELECT crs_code FROM physical_station WHERE crs_code IS NOT NULL`
    );
    const fileList = new Set(Object.keys(stationCoordinates));

    const missing: string[] = [];
    for (const { crs_code } of dbList) {
      if (!fileList.has(crs_code)) {
        missing.push(crs_code);
      }
    }

    const [results] = await this.db.query<StationInfo>(
      `
      SELECT *
      FROM (SELECT crs_code,
                  tiploc_code,
                  station_name,
                  cate_interchange_status,
                  (CAST(easting AS double) - 10000) * 100  as easting,
                  (CAST(northing AS double) - 60000) * 100 as northing
            FROM physical_station
            WHERE crs_code IS NOT NULL
            GROUP BY crs_code) as stations
      WHERE crs_code IN (?)
    `,
      missing
    );
    return results;
  }
}
