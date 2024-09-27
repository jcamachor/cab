import sqlite3 from 'sqlite3';
import Common from './common.js';
import chalk from 'chalk';

const gb_per_chunk = 1;

class LoadSetupManager {
   constructor() {
      this.query_streams = [];
      this.db = new sqlite3.Database('./load-db.sqlite', (err) => {
         if (err) {
            console.error(err);
         }
      });
   }

   // Read query_streams from disk and gather meta info for all databases
   LoadDatabases(path) {
      this.query_streams = Common.LoadDatabaseMetaInfo(path);
      if (this.query_streams.length === 0) {
         console.log(chalk.red("No query streams were found, make sure the first one is query_stream_0.json"));
         process.exit(0);
      }
   }

   async CreateJobTables() {
      console.log(chalk.cyan("\nCreating load jobs ..."));

      await new Promise((resolve, reject) => {
         this.db.run("create table if not exists Jobs(job_id int, database_id int, scale_factor int, table_name string, chunk_count int, step int, status string);", (err) => {
            if (err) {
               console.warn(err);
               return reject(err);
            }
            resolve();
         });
      });
   }

   // Inserts "load-tasks" into the Jobs table. Each task populates a chunk of a table
   async CreateLoadJobs() {
      let job_id = 1;

      const split_table_into_chunks = (database_id, scale_factor, gb_per_scale_factor, relation_name) => {
         const relation_gb = (scale_factor * gb_per_scale_factor);
         const relation_chunks = Math.ceil(relation_gb / gb_per_chunk);
         return Array.from(Array(relation_chunks).keys()).map(k => [job_id++, database_id, scale_factor, relation_name, relation_chunks, k + 1, 'open']);
      }

      //this.db.run("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => [job_id++, db.database_id, db.scale_factor, 'region', 1, 1, 'open']));
      this.query_streams.forEach(db => {
         this.db.run(
             "insert into Jobs values(?, ?, ?, ?, ?, ?, ?);",
             job_id++, db.database_id, db.scale_factor, 'region', 1, 1, 'open'
         );
      });
      //this.db.run("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => [job_id++, db.database_id, db.scale_factor, 'nation', 1, 1, 'open']));
      this.query_streams.forEach(db => {
         this.db.run(
             "insert into Jobs values(?, ?, ?, ?, ?, ?, ?);",
             job_id++, db.database_id, db.scale_factor, 'nation', 1, 1, 'open'
         );
      });
      //this.db.run("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.023, 'customer')).flat());
      this.query_streams.forEach(db => {
         const chunks = split_table_into_chunks(db.database_id, db.scale_factor, 0.023, 'customer');
         chunks.forEach(chunk => {
            this.db.run(
                "insert into Jobs values(?, ?, ?, ?, ?, ?, ?);",
                chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6]
            );
         });
      });
      //this.db.run("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.725, 'lineitem')).flat());
      this.query_streams.forEach(db => {
         const chunks = split_table_into_chunks(db.database_id, db.scale_factor, 0.725, 'lineitem');
         chunks.forEach(chunk => {
            this.db.run(
                "insert into Jobs values(?, ?, ?, ?, ?, ?, ?);",
                chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6]
            );
         });
      });
      //this.db.run("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.164, 'orders')).flat());
      this.query_streams.forEach(db => {
         const chunks = split_table_into_chunks(db.database_id, db.scale_factor, 0.164, 'orders');
         chunks.forEach(chunk => {
            this.db.run(
                "insert into Jobs values(?, ?, ?, ?, ?, ?, ?);",
                chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6]
            );
         });
      });
      //this.db.run("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.023, 'part')).flat());
      this.query_streams.forEach(db => {
         const chunks = split_table_into_chunks(db.database_id, db.scale_factor, 0.023, 'part');
         chunks.forEach(chunk => {
            this.db.run(
                "insert into Jobs values(?, ?, ?, ?, ?, ?, ?);",
                chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6]
            );
         });
      });
      //this.db.run("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.113, 'partsupp')).flat());
      this.query_streams.forEach(db => {
         const chunks = split_table_into_chunks(db.database_id, db.scale_factor, 0.113, 'partsupp');
         chunks.forEach(chunk => {
            this.db.run(
                "insert into Jobs values(?, ?, ?, ?, ?, ?, ?);",
                chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6]
            );
         });
      });
      //this.db.run("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.001, 'supplier')).flat());
      this.query_streams.forEach(db => {
         const chunks = split_table_into_chunks(db.database_id, db.scale_factor, 0.001, 'supplier');
         chunks.forEach(chunk => {
            this.db.run(
                "insert into Jobs values(?, ?, ?, ?, ?, ?, ?);",
                chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6]
            );
         });
      });
   }
}

async function main() {
   await Common.ConfirmRun();

   const setup_manager = new LoadSetupManager();
   setup_manager.LoadDatabases("../benchmark-gen/query_streams");
   await setup_manager.CreateJobTables();
   await setup_manager.CreateLoadJobs();

   console.log(chalk.cyan("\nNormal program exit: setup " + setup_manager.query_streams.length + " databases :)"));
}

main();
