using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using NameNCrew;


string connectionString = "Server=DESKTOP-N9I8DBU;Database=IMDB;" +
    "integrated security=True;TrustServerCertificate=True;";
Stopwatch sw = new Stopwatch();
sw.Start();



string filenameName = "c:/temp/name.basics.tsv";
string filenameCrew = "c:/temp/title.crew.tsv";
using (SqlConnection sqlConn = new SqlConnection(connectionString))
{
    sqlConn.Open();
    Dictionary<string, int> PrimaryProfession = new Dictionary<string, int>();
    Dictionary<string, int> KnownForTitles = new Dictionary<string, int>();
    BulkSql bulkSql = new BulkSql();

    using (StreamReader reader = new StreamReader(filenameName))
    {
        reader.ReadLine(); //skipper første linje
        int linecount = 0;
        int total = 0;
        int batchSize = 1000000;
        string? line;
        SqlTransaction sqlTrans = sqlConn.BeginTransaction();
        while ((line = reader.ReadLine()) != null)
        {
            string[] values = line.Split('\t');
            if (values.Length == 6)
            {
                try
                {
                    Name name = new Name
                    {
                        Id = int.Parse(values[0].Substring(2)),
                        PrimaryName = values[1],
                        BirthYear = values[2] == "\\N" ? null : int.Parse(values[2]),
                        DeathYear = values[3] == "\\N" ? null : int.Parse(values[3]),
                        primaryProfession = values[4] == "\\N" ? new List<string>() : values[4].Split(',').ToList(),
                        KnownForTitles = values[5] == "\\N" ? new List<string>() : values[5].Split(',').ToList()

                    };
                    bulkSql.InsertName(name);
                    foreach (string profession in name.primaryProfession)
                    {
                        if (!PrimaryProfession.ContainsKey(profession))
                            AddProfession(profession, sqlConn, sqlTrans, PrimaryProfession);

                        bulkSql.InsertProfessionName(name.Id, PrimaryProfession[profession]); //skal rettes så den indsætter i profession i stedet // -Isak: Tror jeg ikke, da det burde ske i Add Professions nu når den virker
                    }

                    foreach (string knownForName in name.KnownForTitles)
                    {
                        if (!KnownForTitles.ContainsKey(knownForName))
                            AddKnownForTitle(knownForName, KnownForTitles); 
                         
                        bulkSql.InsertKnownFor(name.Id, KnownForTitles[knownForName]); 
                    }

                    linecount++;
                    total++;
                    if (linecount >= batchSize)
                    {
                        //indsætter batch i db
                        //SqlCommand cmd = new SqlCommand("SET IDENTITY_INSERT Name ON;", sqlConn, sqlTrans);
                        //cmd.ExecuteNonQuery();
                        bulkSql.InsertIntoDB(sqlConn, sqlTrans, insertNames: true);
                        bulkSql.ClearTables();


                        //cmd = new SqlCommand("SET IDENTITY_INSERT Name OFF;", sqlConn, sqlTrans);
                        //cmd.ExecuteNonQuery();
                        linecount = 0;
                        sqlTrans.Commit();
                        sqlTrans = sqlConn.BeginTransaction();
                    }
                }
                catch (SqlException sqlEx)
                {
                    Console.WriteLine("SQL error inserting line: " + line);
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
                Console.WriteLine("Not 9 values: " + line);
            }
        }
        if (linecount > 0)
        {
            //SqlCommand cmd = new SqlCommand("SET IDENTITY_INSERT Name ON;", sqlConn, sqlTrans);
            //cmd.ExecuteNonQuery();

            bulkSql.InsertIntoDB(sqlConn, sqlTrans, insertNames: true);
            bulkSql.ClearTables();

            //cmd = new SqlCommand("SET IDENTITY_INSERT Name OFF;", sqlConn, sqlTrans);
            //cmd.ExecuteNonQuery();
            sqlTrans.Commit();

        }
    }
    sw.Stop();

    Console.WriteLine("Millisekunder: " + sw.ElapsedMilliseconds);
    Console.WriteLine("Alle records: " + 1200 * sw.ElapsedMilliseconds);
    Console.WriteLine("Alle records i timer: " + (1200.0 * sw.ElapsedMilliseconds) / 1000.0 / 60.0 / 60.0);
}

using (SqlConnection sqlConn = new SqlConnection(connectionString))
{
    sqlConn.Open();
    Dictionary<string, int> PrimaryProfession = new Dictionary<string, int>();
    BulkSql bulkSql = new BulkSql();

    using (StreamReader reader = new StreamReader(filenameCrew))
    {
        reader.ReadLine(); //skipper første linje
        int linecount = 0;
        int total = 0;
        int batchSize = 1000000;
        string? line;
        SqlTransaction sqlTrans = sqlConn.BeginTransaction();
        while ((line = reader.ReadLine()) != null)
        {
            string[] values = line.Split('\t');
            if (values.Length == 3)
            {
                try
                {
                    int titleId = int.Parse(values[0].Substring(2)); // tt0000001 -> 1

                    // directors
                    if (values[1] != "\\N")
                    {
                        foreach (string dir in values[1].Split(','))
                        {
                            int nameId = int.Parse(dir.Substring(2));
                            bulkSql.InsertTitleDirector(titleId, nameId);
                        }
                    }

                    // writers
                    if (values[2] != "\\N")
                    {
                        foreach (string writer in values[2].Split(','))
                        {
                            int nameId = int.Parse(writer.Substring(2));
                            bulkSql.InsertTitleWriter(titleId, nameId);
                        }
                    }

                    linecount++;
                    total++;
                    if (linecount >= batchSize)
                    {
                        bulkSql.InsertIntoDB(sqlConn, sqlTrans, insertCrew: true);
                        bulkSql.ClearTables();


                        linecount = 0;
                        sqlTrans.Commit();
                        sqlTrans = sqlConn.BeginTransaction();
                    }
                }
                catch (SqlException sqlEx)
                {
                    Console.WriteLine("SQL error inserting line: " + line);
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
                Console.WriteLine("Not 9 values: " + line);
            }
        }
        if (linecount > 0)
        {
            bulkSql.InsertIntoDB(sqlConn, sqlTrans, insertCrew: true);
            bulkSql.ClearTables();

            sqlTrans.Commit();
        }
    }
    sw.Stop();

    Console.WriteLine("Millisekunder: " + sw.ElapsedMilliseconds);
    Console.WriteLine("Alle records: " + 1200 * sw.ElapsedMilliseconds);
    Console.WriteLine("Alle records i timer: " + (1200.0 * sw.ElapsedMilliseconds) / 1000.0 / 60.0 / 60.0);
}


void AddProfession(string primaryProfession, SqlConnection sqlConn, SqlTransaction sqlTrans, Dictionary<string, int> PrimaryProfession)
{
    if (!PrimaryProfession.ContainsKey(primaryProfession))
    {
        SqlCommand sqlComm = new SqlCommand(
            "INSERT INTO Profession (Profession) VALUES ('" + primaryProfession + "'); " + 
            "SELECT SCOPE_IDENTITY();", sqlConn, sqlTrans); 
        int newId = Convert.ToInt32(sqlComm.ExecuteScalar());
        PrimaryProfession[primaryProfession] = newId;
    }
}

void AddKnownForTitle(string knownForTitle, Dictionary<string, int> KnownForTitles)
{
    if (!KnownForTitles.ContainsKey(knownForTitle))
    {
        // Convert ttNNNNNNN -> int id
        int titleId = int.Parse(knownForTitle.Substring(2));
        KnownForTitles[knownForTitle] = titleId;
    }
}