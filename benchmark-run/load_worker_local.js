import sqlite3 from 'sqlite3';
import Common from './common.js';
import fs from "fs";
import path from "path";
import exec from 'child_process';
import chalk from 'chalk';

const dbgen_file = "dbgen";
const dbgen_folder = "../../tpch-dbgen";
const dbgen_path = dbgen_folder + "/" + dbgen_file;
const csv_directory = "gen_" + process.pid;
const data_folder = "./data";

class LoadWorker {
   constructor() {
      this.db = new sqlite3.Database('./load-db.sqlite', (err) => {
         if (err) {
            console.error(err);
         }
      });
      this.stats = {
         compress_total: 0,
         dbgen_total: 0,
         upload_total: 0,
         copy_local_total: 0,
         copy_athena_total: 0,
         copy_snowflake_total: 0,
         copy_bigquery_total: 0,
         pipelined_upload_total: 0,
         compress: [],
         dbgen: [],
         upload: [],
         copy_local: [],
         copy_athena: [],
         copy_snowflake: [],
         copy_bigquery: [],
         pipelined_upload: [],
      };
   }

   async GetNextJob() {
      console.log(chalk.cyan("\nGetting next job ..."));
      while (true) {
         let jobs = await new Promise((resolve, reject) => {
            this.db.all("select * from jobs where status = 'open' limit 1", (err, res) => {
               if (err) {
                  console.warn(err);
                  return reject(err);  // Handle the error by rejecting the promise
               }
               resolve(res);  // Resolve the promise with the result
            });
         });
         if (!jobs || jobs.length === 0) {
            console.log("No more open jobs -> done");
            return null
         }
         const job = jobs[0];

         let updated_rows = await new Promise((resolve, reject) => {
            this.db.all(
                "update jobs set status = 'running' where job_id = ? and status = 'open' RETURNING *",
                job['job_id'],
                function (err, res) {
                   if (err) {
                      console.warn(err);
                      return reject(err);  // Reject the promise if there's an error
                   }
                   resolve(res.length);
                }
            );
         });
         if (updated_rows && updated_rows === 1) {
            console.log(job);
            return job;
         }
      }
   }

   async _EnsureThatDatabaseGeneratorIsAvailable() {
      return new Promise((r) => {
         fs.access(dbgen_path, fs.constants.X_OK, (err) => {
            if (err) {
               const error_message = "Can not execute '" + dbgen_path + "'.\n" +
                  "Make sure the tpc-h dbgen tool is installed in '" + dbgen_folder + "'.\n" +
                  "If it is located at a different location, you can adjust the `dbgen_folder` in this script.\n" +
                  "Otherwise download and compile it: https://github.com/alexandervanrenen/tpch-dbgen";
               console.log(chalk.red(error_message));
               throw new Error(error_message);
            }
            r();
         });
      });
   }

   _TableNameToDbGenTableCode(table_name) {
      switch (table_name) {
         case "customer": { return "c"; }
         case "lineitem": { return "L"; }
         case "nation": { return "n"; }
         case "orders": { return "O"; }
         case "part": { return "P"; }
         case "region": { return "r"; }
         case "supplier": { return "s"; }
         case "partsupp": { return "S"; }
      }
   }

   async _RunCommand(command, args) {
      console.log(command + " " + args.join(" "));
      const start = Date.now();
      return new Promise((r) => {
         const child = exec.spawn(command, args);
         child.stdout.on('data', (data) => { console.log(Common.FormatChildOutput(command, data.toString())); });
         child.stderr.on('data', (data) => { console.log(Common.FormatChildOutput(command, data.toString())); });
         child.on('close', (code, signal) => {
            if (code !== 0) throw new Error("non zero exit code from " + command);
            r((Date.now() - start));
         });
         child.on('error', (err) => { throw err; });
      });
   }

   async CreateCsvFile(job) {
      const start = Date.now();

      // Check that dbgen is fine
      await this._EnsureThatDatabaseGeneratorIsAvailable(dbgen_path);
      if (!fs.existsSync(csv_directory)) {
         fs.mkdirSync(csv_directory);
      }

      // Create
      const args = ["-s", job['scale_factor'], "-T", this._TableNameToDbGenTableCode(job['table_name']), "-f", "-b", dbgen_folder + "/dists.dss"];
      if (job['chunk_count'] > 1) {
         args.push("-C", job['chunk_count'], "-S", job['step']);
      }
      console.log(chalk.cyan("\nGenerating table chunk ..."));
      process.env.DSS_PATH = csv_directory;
      await this._RunCommand(dbgen_path, args);
      this.stats.dbgen_total += (Date.now() - start);
      this.stats.dbgen.push({time: Date.now() - start, table_name: job['table_name']});

      // Set permissions, because sometimes they are not correct :/
      await this._RunCommand("chmod", ["u+rw", this._GetGeneratedFileName(job)]);
   }

   _GetGeneratedFileName(job) {
      return process.env.PWD + "/" + csv_directory + "/" + job['table_name'] + ".tbl" + (job['chunk_count'] > 1 ? "." + job['step'] : "");
   }

   async CopyIntoFolder(job) {
      const start = Date.now();
      const sourceFilePath = this._GetGeneratedFileName(job);
      // Create the subfolder structure: database_id/table_name
      const destinationFolderPath = path.join(data_folder, String(job['database_id']), job['table_name']);
      // Ensure the folder exists, create it if it doesn't
      if (!fs.existsSync(destinationFolderPath)) {
         fs.mkdirSync(destinationFolderPath, { recursive: true });
      }
      // Extract the file name from the source file path
      const fileName = path.basename(sourceFilePath);
      // Define the destination path using the extracted file name
      const destinationFilePath = path.join(destinationFolderPath, fileName);
      // Copy the file from the source (simulating) to the local folder
      fs.copyFileSync(sourceFilePath, destinationFilePath);
      console.log(chalk.cyan("\nFile copied to final local folder ..."));
      console.log(`Copied to: ${destinationFilePath}`);
      // Capture and log stats for the operation
      this.stats.copy_local_total += (Date.now() - start);
      this.stats.copy_local.push({ time: Date.now() - start, table_name: job['table_name'] });
   }

   async MarkJobAsDone(job) {
      console.log(chalk.cyan("\nMarking job as complete ..."));
      let updated_rows = await new Promise((resolve, reject) => {
         this.db.all(
             "update jobs set status = 'complete' where job_id = ? and status = 'running' RETURNING *",
             job['job_id'],
             function (err, res) {
                if (err) {
                   console.warn(err);
                   return reject(err);  // Reject the promise if there's an error
                }
                resolve(res.length);
             }
         );
      });
      if (!updated_rows || updated_rows !== 1) {
         throw new Error("Unexpected result on job completion: " + JSON.stringify(updated_rows));
      }
      console.log("done");
   }
}

async function main() {
   const worker = new LoadWorker();

   let job;
   while (job = await worker.GetNextJob()) {
      await worker.CreateCsvFile(job);
      await worker.CopyIntoFolder(job);
      await worker.MarkJobAsDone(job);
   }

   console.log(chalk.cyan("\nNormal program exit: done :)"));
   console.log(JSON.stringify(worker.stats));
}

main();
