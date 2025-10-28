using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using NameNCrew;


string connectionString = "Server=BIRKPC;Database=IMDB;" +
    "integrated security=True;TrustServerCertificate=True;";
Stopwatch sw = new Stopwatch();
sw.Start();



string filename = "c:/temp/title.basics.tsv";
using (SqlConnection sqlConn = new SqlConnection(connectionString))
{
    sqlConn.Open();
    Dictionary<string, int> PrimaryProfession = new Dictionary<string, int>();
    Dictionary<string, int> KnownForTitle = new Dictionary<string, int>();
    BulkSql bulkSql = new BulkSql();

    using (StreamReader reader = new StreamReader(filename))
    {
        reader.ReadLine(); //skipper første linje
        int linecount = 0;
        int total = 0;
        int batchSize = 100000;
        string? titleString;
        SqlTransaction sqlTrans = sqlConn.BeginTransaction();
        while ((titleString = reader.ReadLine()) != null)
        {
            string[] values = titleString.Split('\t');
            if (values.Length == 9)
            {
                if (!PrimaryProfession.ContainsKey(values[1]))
                { 
                    AddTitleType(values[1], sqlConn, sqlTrans, PrimaryProfession);
                }
                try
                {
                    Name name = new Name
                    {
                        Id = int.Parse(values[0].Substring(2)),
                        PrimaryName = values[1],
                        BirthYear = values[2] == "\\N" ? null : int.Parse(values[2]),
                        DeathYear = values[3] == "\\N" ? null : int.Parse(values[3]),
                        primaryProfession = values[4] == "\\N" ? new List<string>() : values[4].Split(',').ToList(),
                        KnownForTitles= values[5] == "\\N" ? new List<string>() : values[5].Split(',').ToList()

                    };
                    bulkSql.InsertName(name);
                    foreach (string profession in name.primaryProfession)
                    {
                        if (!PrimaryProfession.ContainsKey(profession))
                            AddProfession(profession, sqlConn, sqlTrans, PrimaryProfession);

                        bulkSql.InsertProfession(name.Id, PrimaryProfession[profession]);
                    }

                    linecount++;
                    total++;
                    if (linecount >= batchSize)
                    {
                        //indsætter batch i db
                        SqlCommand cmd = new SqlCommand("SET IDENTITY_INSERT Titles ON;", sqlConn, sqlTrans);
                        cmd.ExecuteNonQuery();
                        bulkSql.InsertIntoDB(sqlConn, sqlTrans);
                        bulkSql.ClearTables();


                        cmd = new SqlCommand("SET IDENTITY_INSERT Titles OFF;", sqlConn, sqlTrans);
                        cmd.ExecuteNonQuery();
                        linecount = 0;
                        sqlTrans.Commit();
                        sqlTrans = sqlConn.BeginTransaction();
                    }
                }
                catch (SqlException sqlEx)
                {
                    Console.WriteLine("SQL error inserting line: " + titleString);
                    Console.WriteLine("Error Number: " + sqlEx.Number);
                    Console.WriteLine("Error Message: " + sqlEx.Message);
                    Console.WriteLine("Error Procedure: " + sqlEx.Procedure);
                    Console.WriteLine("Line Number: " + sqlEx.LineNumber);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Other error: " + ex.Message);
                }
            }
            else
            {
                Console.WriteLine("Not 9 values: " + titleString);
            }
        }
        if (linecount > 0)
        {
            SqlCommand cmd = new SqlCommand("SET IDENTITY_INSERT Titles ON;", sqlConn, sqlTrans);
            cmd.ExecuteNonQuery();

            bulkSql.InsertIntoDB(sqlConn, sqlTrans);
            bulkSql.ClearTables();

            cmd = new SqlCommand("SET IDENTITY_INSERT Titles OFF;", sqlConn, sqlTrans);
            cmd.ExecuteNonQuery();
            sqlTrans.Commit();

        }
    }
    sw.Stop();

    Console.WriteLine("Millisekunder: " + sw.ElapsedMilliseconds);
    Console.WriteLine("Alle records: " + 1200 * sw.ElapsedMilliseconds);
    Console.WriteLine("Alle records i timer: " + (1200.0 * sw.ElapsedMilliseconds) / 1000.0 / 60.0 / 60.0);





    void AddProfession(string titleType, SqlConnection sqlConn, SqlTransaction sqlTrans, Dictionary<string, int> PrimaryProfession)
    {
        if (!PrimaryProfession.ContainsKey(PrimaryProfession))
        {
            SqlCommand sqlComm = new SqlCommand(
                "INSERT INTO TitleTypes (Type) VALUES ('" + PrimaryProfession + "'); " +
                "SELECT SCOPE_IDENTITY();", sqlConn, sqlTrans);
            int newId = Convert.ToInt32(sqlComm.ExecuteScalar());
            PrimaryProfession[titleType] = newId;
        }
    }
}