using System;
using System.Collections.Generic;
using System.Text;
using Amazon.S3.Transfer;
using Amazon.S3;
using System.IO;

namespace AWSLambda
{
    class AWSManager
    {
        public static void UploadFile(MemoryStream parquetFile, string bucketName, string keyName)
        {
            using (var fileTransferUtility = new TransferUtility(new AmazonS3Client()))
            {
                fileTransferUtility.Upload(parquetFile, bucketName, keyName + ".parquet");
            }
            parquetFile.Close();
        }

    }
}
