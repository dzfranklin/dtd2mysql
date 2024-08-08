import * as fs from "fs";
import {GTFSOutput} from "./GTFSOutput";
import {Writable} from "stream";
import { stringify } from "csv-stringify";

export class FileOutput implements GTFSOutput {
  /**
   * Wrapper around file output library that returns a file as a WritableStream
   */
  public open(filename: string): Writable {
    const writer = stringify({ header: true });
    writer.pipe(fs.createWriteStream(filename));
    return writer;
  }

  /**
   * Nothing to do, files already closed
   */
  public end(): void {

  }

}
