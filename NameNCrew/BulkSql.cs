using Microsoft.Data.SqlClient;
using Microsoft.Identity.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace NameNCrew {
    public class BulkSql {

        public DataTable NameTable { get; set; }
        public DataTable ProfessionNameTable { get; set; }
        public DataTable TitleDirectorTable { get; set; }
        public DataTable TitleWriterTable { get; set; }
        public DataTable TitleNameTable { get; set; }
        public DataTable KnownForTable { get; set; }

        public BulkSql()
        {
            NameTable = new DataTable();
            NameTable.Columns.Add("Id", typeof(int));
            NameTable.Columns.Add("PrimaryName", typeof(string));
            NameTable.Columns.Add("BirthYear", typeof(short));
            NameTable.Columns.Add("DeathYear", typeof(short));

            ProfessionNameTable = new DataTable();
            ProfessionNameTable.Columns.Add("ProfessionId", typeof(int));
            ProfessionNameTable.Columns.Add("NameId", typeof(int));

            TitleDirectorTable = new DataTable();
            TitleDirectorTable.Columns.Add("TitleId", typeof(int));
            TitleDirectorTable.Columns.Add("DirectorId", typeof(int));

            TitleWriterTable = new DataTable();
            TitleWriterTable.Columns.Add("TitleId", typeof(int));
            TitleWriterTable.Columns.Add("WriterId", typeof(int));

            TitleNameTable = new DataTable();
            TitleNameTable.Columns.Add("TitleId", typeof(int));
            TitleNameTable.Columns.Add("NameId", typeof(int));

            KnownForTable = new DataTable();
            KnownForTable.Columns.Add("NameId", typeof(int));
            KnownForTable.Columns.Add("TitleId", typeof(int));
        }

        public void InsertName(Name name)
        {
            DataRow row = NameTable.NewRow();
            row["Id"] = name.Id;
            row["PrimaryName"] = name.PrimaryName;
            row["BirthYear"] = (object?)name.BirthYear ?? DBNull.Value;
            row["DeathYear"] = (object?)name.DeathYear ?? DBNull.Value;
            NameTable.Rows.Add(row);
        }

        public void InsertProfessionName(int professionId, int nameId)
        {
            DataRow row = ProfessionNameTable.NewRow();
            row["ProfessionId"] = professionId;
            row["NameId"] = nameId;
            ProfessionNameTable.Rows.Add(row);
        }

        public void InsertTitleDirector(int titleId, int directorId)
        {
            DataRow row = TitleDirectorTable.NewRow();
            row["TitleId"] = titleId;
            row["DirectorId"] = directorId;
            TitleDirectorTable.Rows.Add(row);
        }

        public void InsertTitleWriter(int titleId, int writerId)
        {
            DataRow row = TitleWriterTable.NewRow();
            row["TitleId"] = titleId;
            row["WriterId"] = writerId;
            TitleWriterTable.Rows.Add(row);
        }

        public void InsertTitleName(int titleId, int nameId)
        {
            DataRow row = TitleWriterTable.NewRow();
            row["TitleId"] = titleId;
            row["NameId"] = nameId;
            TitleWriterTable.Rows.Add(row);
        }

        public void InsertKnownFor(int nameId, int titleId)
        {
            DataRow row = KnownForTable.NewRow();
            row["NameId"] = nameId;
            row["TitleId"] = titleId;
            KnownForTable.Rows.Add(row);
        }

        public void InsertIntoDB(SqlConnection sqlConn, SqlTransaction sqlTrans, bool insertNames = false, bool insertCrew = false)
        {
            if (insertNames)
            {
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans))
                {
                    bulkCopy.DestinationTableName = "Name";
                    bulkCopy.BatchSize = 500000;
                    bulkCopy.BulkCopyTimeout = 0;
                    bulkCopy.ColumnMappings.Add("Id", "Id");
                    bulkCopy.ColumnMappings.Add("PrimaryName", "PrimaryName");
                    bulkCopy.ColumnMappings.Add("BirthYear", "BirthYear");
                    bulkCopy.ColumnMappings.Add("DeathYear", "DeathYear");
                    bulkCopy.WriteToServer(NameTable);
                }

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepNulls, sqlTrans))
                {
                    bulkCopy.DestinationTableName = "Profession_Name";
                    bulkCopy.ColumnMappings.Add("ProfessionId", "ProfessionId");
                    bulkCopy.ColumnMappings.Add("NameId", "NameId");
                    bulkCopy.WriteToServer(ProfessionNameTable);
                }

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepNulls, sqlTrans))
                {
                    bulkCopy.DestinationTableName = "Title_Name";
                    bulkCopy.ColumnMappings.Add("NameId", "NameId");
                    bulkCopy.ColumnMappings.Add("TitleId", "TitleId");
                    bulkCopy.WriteToServer(KnownForTable);
                }
            }

            if (insertCrew)
            {
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepNulls, sqlTrans))
                {
                    bulkCopy.DestinationTableName = "Title_Director";
                    bulkCopy.ColumnMappings.Add("TitleId", "TitleId");
                    bulkCopy.ColumnMappings.Add("DirectorId", "DirectorId");
                    bulkCopy.WriteToServer(TitleDirectorTable);
                }

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepNulls, sqlTrans))
                {
                    bulkCopy.DestinationTableName = "Title_Writer";
                    bulkCopy.ColumnMappings.Add("TitleId", "TitleId");
                    bulkCopy.ColumnMappings.Add("WriterId", "WriterId");
                    bulkCopy.WriteToServer(TitleWriterTable);
                }
            } 
        }

        public void ClearTables()
        {
            TitleDirectorTable.Clear();
            TitleWriterTable.Clear();
            ProfessionNameTable.Clear();
            NameTable.Clear();
            TitleNameTable.Clear();
            KnownForTable.Clear();
        }

    }
}

