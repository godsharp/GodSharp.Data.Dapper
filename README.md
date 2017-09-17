# GodSharp.Data.Dapper
DbContext for Dapper.

[![AppVeyor build status](https://img.shields.io/appveyor/ci/seayxu/godsharp-data-dapper.svg?label=appveyor&style=flat-square)](https://ci.appveyor.com/project/seayxu/godsharp-data-dapper/) [![NuGet](https://img.shields.io/nuget/v/GodSharp.Data.Dapper.svg?label=nuget&style=flat-square)](https://www.nuget.org/packages/GodSharp.Data.Dapper/) [![MyGet](https://img.shields.io/myget/godsharp/v/GodSharp.Data.Dapper.svg?label=myget&style=flat-square)](https://www.myget.org/Package/Details/godsharp?packageType=nuget&packageId=GodSharp.Data.Dapper)

# Supported .NET Version
- .NET Framework 3.5+
- .NET Standard 1.3
- .NET Standard 2.0

# Getting Started

1. Install Nuget Package.

  See [here](https://www.nuget.org/packages/GodSharp.Data.Dapper/).

2. Inherited class `DbContext`.

  Use connectionString name in `App.Config` or `Web.config` as constructor parameters.

  If your project type is `.NET Core`,you should add json setting for `connectionStrings` and `DbConnectionFactories`,format reference [here](https://github.com/godsharp/GodSharp.Data.Common.DbProvider#getting-started).

```
public class IRepository:DbContext
{
    public IRepository():base("db")
    {
    }
}
```

  Or override `OnConfiguration` method, invoke `Use` method.

```
public class IRepository : DbContext
{
    public IRepository()
    {
    }

    protected override void OnConfiguration(DbConnectionFactory factory)
    {
        base.OnConfiguration(factory);
        string providerName= "MySql.Data.MySqlClient";
        string connectionString= "Data Source=127.0.0.1;Initial Catalog=user;User Id=root;Password=root;Charset=utf8;";
        factory.Use(providerName, connectionString);
    }
}
```

3. Define data objects.

```
public class TestRepository : IRepository
{
	public bool Connected()
	{
		try
		{
			object obj = ExecuteScalar("SELECT CONVERT(varchar(50), GETDATE(), 25);", null, null, CommandType.Text);

			if (obj == null)
			{
				return false;
			}

			string dt = obj.ToString();

			DateTime _dt;

			return DateTime.TryParse(dt, out _dt);
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}
}
```

  Execute Sql with transaction.

```
public class TestRepository : IRepository
{
	public bool ExecuteWithTran()
    {
        string sql = null;
        object param = null;

        // begin tran
        BeginTransaction();

        // execute sql
        Execute(sql, param);
            
        // commit tran
        Commit();

        // or
        // callback tran
        //Callback();
    }
}
```

# License

  Licensed under the MIT License.