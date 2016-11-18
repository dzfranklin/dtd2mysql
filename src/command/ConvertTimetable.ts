
import Command from "./Command";
import Container from "./Container";

const fs = require("fs");
const NATIVE_SCHEMA = __dirname + "/../../asset/native-schema.sql";

export default class ConvertTimetable implements Command {
    private db;
    private logger;

    constructor(container: Container) {
        this.db = container.get("database");
        this.logger = container.get("logger");
    }

    async run(argv: string[]) {
        await Promise.all(this.createSchema());

        this.logger("Converting feed");
        const copyTimetable = this.db.query(`
            SET @prevDepart := '00:00:00';
            SET @prevStation := '   ';
            INSERT INTO timetable_connection
            SELECT @prevDepart, arrival_time, IF (stop_sequence = 1, '   ', @prevStation), parent_station, train_uid, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date, atoc_code, IF (train_category='BS' OR train_category='BR', 'bus', 'train'), @prevStation := parent_station, @prevDepart := departure_time
            FROM stop_times
            LEFT JOIN stops USING (stop_id)
            LEFT JOIN trips USING (trip_id)
            LEFT JOIN calendar USING (service_id)
            ORDER BY trip_id, stop_sequence
        `);

        const clean = this.db.query(`
            DELETE FROM timetable_connection WHERE origin = '   ' OR origin = destination OR departure_time > arrival_time
        `);

        const copyInterchange = this.db.query(`
            INSERT INTO interchange SELECT from_stop_id, min_transfer_time FROM transfers        
        `);

        return Promise.all([copyTimetable, clean, copyInterchange]);
    }

    private createSchema(): Promise<any>[] {
        const nativeSchema = fs.readFileSync(NATIVE_SCHEMA, "utf-8");

        return nativeSchema.split(";").map(this.db.query);
    }

}