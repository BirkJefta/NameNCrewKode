using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NameNCrew {
    public class Name {
        public int Id { get; set; }
        public string PrimaryName { get; set; }
        public int? BirthYear { get; set; }
        public int? DeathYear { get; set; }
        public List<string> primaryProfession { get; set; }
        public List<string> KnownForTitles { get; set; }

        public Name()
        {
            primaryProfession = new List<string>();
            KnownForTitles = new List<string>();
        }

        public string ToSQL()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append($"INSERT INTO Name (Id, PrimaryName, BirthYear, DeathYear) " +
                $"VALUES (");
            sb.Append($"{Id}, ");
            sb.Append($"'{PrimaryName.Replace("'", "''")}', ");
            sb.Append(BirthYear.HasValue ? $"{BirthYear.Value}, " : "NULL, ");
            sb.Append(DeathYear.HasValue ? $"{DeathYear.Value}, " : "NULL, ");
            sb.Append(");");

            return sb.ToString();
        }
    }
}
