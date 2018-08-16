using Xunit;

namespace GodSharp.Data.Dapper.UnitTest
{
    public class DbContextTest
    {
        [Fact]
        public void Test1()
        {          
        }
    }

    public class Sample1DapperDbContext : DapperDbContext
    {
        public Sample1DapperDbContext() : base("")
        {
        }
    }

    public class Sample2DapperDbContext : DapperDbContext
    {
        public Sample2DapperDbContext(string name) : base(name)
        {
        }
    }

    public class Sample3DapperDbContext : DapperDbContext
    {
        protected override void OnConfiguration(DbConnectionFactory factory)
        {
            base.OnConfiguration(factory);

            // SQL Server
            factory.Use("System.Data.SqlClient", "Data Source=localhost;Initial Catalog=master;User ID=sa;Password=1234;");

            // MySql with Pomelo
            factory.Use("Pomelo.Data.MySql", "Data Source=localhost;Initial Catalog=user;User ID=root;Password=1234;");

            // Sqlite
            factory.Use("Microsoft.Data.Sqlite", "Data Source=data.db;");

            // PostgreSql
            factory.Use("Npgsql", "Host=localhost;Database=postgres;Username=postgres;Password=1234;");
        }
    }
}
