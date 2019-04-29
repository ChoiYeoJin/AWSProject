using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace AWSLambda
{
    class FileManager
    {
        public static char delimiter = ',';
        public static int lineLimit = 100000;
        public static void EventToParquet(StreamReader reader, string key)
        {
            var schema = new Schema(
                                new DataField<string>("identity_adid"),
                                new DataField<string>("os"),
                                new DataField<string>("model"),
                                new DataField<string>("country"),
                                new DataField<string>("event_name"),
                                new DataField<string>("log_id"),
                                new DataField<string>("server_datetime"),
                                new DataField<int?>("quantity"),
                                new DataField<float?>("price"));
            var parquetTable = new Parquet.Data.Rows.Table(schema);

            string line = "";
            int i;
            float d;
            int lineCount = 1;
            while ((line = reader.ReadLine()) != null)
            {
                if (lineCount % lineLimit == 0)
                {
                    AWSManager.UploadFile(WriteParquet(schema, parquetTable),
                        "converted-data/event", key + (lineCount / lineLimit).ToString());
                    parquetTable.Clear();
                    parquetTable = new Parquet.Data.Rows.Table(schema);
                }
                if (!string.IsNullOrEmpty(line))
                {
                    line = line.Replace("\"", "");
                    var splited = line.Split(delimiter);
                    parquetTable.Add(new Parquet.Data.Rows.Row(splited[0], splited[1], splited[2], splited[3], splited[4], splited[5],
                        splited[6], int.TryParse(splited[7], out i) ? (int?)i : null,
                        float.TryParse(splited[8], out d) ? (float?)d : null));
                }
                lineCount++;
            }
            if (parquetTable.Count != 0)
                AWSManager.UploadFile(WriteParquet(schema, parquetTable),
                        "converted-data/event", key + (lineCount / lineLimit+1).ToString());
            reader.Close();
        }

        public static void AttrToParquet(StreamReader reader, string key)
        {
            var schema = new Schema(
                    new DataField<string>("partner"),
                    new DataField<string>("campaign"),
                    new DataField<string>("server_datetime"),
                    new DataField<string>("tracker_id"),
                    new DataField<string>("log_id"),
                    new DataField<int?>("attribution_type"),
                    new DataField<string>("identity_adid"));
            var parquetTable = new Parquet.Data.Rows.Table(schema);

            string line = "";
            int i;
            int lineCount = 1;
            while ((line = reader.ReadLine()) != null)
            {
                if(lineCount%lineLimit == 0)
                {
                    AWSManager.UploadFile(WriteParquet(schema, parquetTable),
                        "converted-data/attr", key + (lineCount / lineLimit).ToString());
                    parquetTable.Clear();
                    parquetTable = new Parquet.Data.Rows.Table(schema);
                }
                if (!string.IsNullOrEmpty(line))
                {
                    line = line.Replace("\"", "");
                    var splited = line.Split(delimiter);
                    parquetTable.Add(new Parquet.Data.Rows.Row(splited[0], splited[1], splited[2],
                        splited[3], splited[4], int.TryParse(splited[5], out i) ? (int?)i : null, splited[6]));
                }
                lineCount++;
            }
            
            if(parquetTable.Count != 0)
                AWSManager.UploadFile(WriteParquet(schema, parquetTable),
                    "converted-data/attr", "part-00000-" + (lineCount / lineLimit+1).ToString());
            reader.Close();
        }

        public static MemoryStream WriteParquet(Schema schema, Parquet.Data.Rows.Table parquetTable)
        {
            MemoryStream fileStream = new MemoryStream();
                using (var parquetWriter = new Parquet.ParquetWriter(schema, fileStream))
                {
                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        groupWriter.Write(parquetTable);
                    }

                }
            return fileStream;
            
        }

    }
}
