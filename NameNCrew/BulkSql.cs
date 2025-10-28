using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NameNCrew {
    public class BulkSql {

        public DataTable TitleDataTable { get; set; }
        public DataTable TitleGenreDataTable { get; set; }
        public BulkSql()
        {
            TitleDataTable = new DataTable();
            TitleDataTable.Columns.Add("Id", typeof(int));
            TitleDataTable.Columns.Add("PrimaryName", typeof(string));
            TitleDataTable.Columns.Add("BirthYear", typeof(short));
            TitleDataTable.Columns.Add("DeathYear", typeof(short));

            TitleGenreDataTable = new DataTable();
            TitleGenreDataTable.Columns.Add("TitleId", typeof(int));
            TitleGenreDataTable.Columns.Add("GenreId", typeof(int));
        }

        public void InsertName(Name name)
        {
            DataRow row = TitleDataTable.NewRow();
            row["Id"] = name.Id;
            row["PrimaryName"] = name.PrimaryName;
            row["BirthYear"] = (object?)name.BirthYear ?? DBNull.Value;
            row["DeathYear"] = (object?)name.DeathYear ?? DBNull.Value;
            TitleDataTable.Rows.Add(row);
        }

        public void InsertProfession(int titleId, int genreId)
        {
            DataRow row = TitleGenreDataTable.NewRow();
            row["TitleId"] = titleId;
            row["GenreId"] = genreId;
            TitleGenreDataTable.Rows.Add(row);
        }

        public void InsertIntoDB(SqlConnection sqlConn, SqlTransaction sqlTrans)
        {

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans))
            {
                bulkCopy.DestinationTableName = "Titles";
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.BatchSize = 500000;
                bulkCopy.ColumnMappings.Add("Id", "Id");
                bulkCopy.ColumnMappings.Add("TypeId", "TypeId");
                bulkCopy.ColumnMappings.Add("PrimaryTitle", "PrimaryTitle");
                bulkCopy.ColumnMappings.Add("OriginalTitle", "OriginalTitle");
                bulkCopy.ColumnMappings.Add("IsAdult", "IsAdult");
                bulkCopy.ColumnMappings.Add("StartYear", "StartYear");
                bulkCopy.ColumnMappings.Add("EndYear", "EndYear");
                bulkCopy.ColumnMappings.Add("RuntimeMinutes", "RuntimeMinutes");
                bulkCopy.WriteToServer(TitleDataTable);
            }
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepNulls, sqlTrans))
            {
                bulkCopy.DestinationTableName = "Title_Genre";
                bulkCopy.ColumnMappings.Add("TitleId", "TitleId");
                bulkCopy.ColumnMappings.Add("GenreId", "GenreId");
                bulkCopy.WriteToServer(TitleGenreDataTable);
            }
        }
        public void ClearTables()
        {
            TitleDataTable.Clear();
            TitleGenreDataTable.Clear();
        }

    }
}

