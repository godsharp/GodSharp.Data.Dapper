using System;
#if NFX
using System.Configuration; 
#endif
using System.Data;
using System.Data.Common;
#if CFX
using GodSharp.Data.Common.DbProvider;
// ReSharper disable SuggestVarOrType_BuiltInTypes
#endif
#if NET35
using GodSharp.Data.Dapper.Extension;
#endif

namespace GodSharp.Data.Dapper
{
    /// <summary>
    /// DbConnection Factory
    /// </summary>
    public class DbConnectionFactory
    {
        /// <summary>
        /// Gets or sets the database provider factory.
        /// </summary>
        /// <value>
        /// The database provider factory.
        /// </value>
        public DbProviderFactory DbProviderFactory { get; private set; }

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
        /// <value>
        /// The connection string.
        /// </value>
        public string ConnectionString { get; private set; }

        /// <summary>
        /// Gets or sets the name of the provider.
        /// </summary>
        /// <value>
        /// The name of the provider.
        /// </value>
        public string ProviderName { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DbConnectionFactory"/> class.
        /// </summary>
        public DbConnectionFactory()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DbConnectionFactory"/> class.
        /// </summary>
        /// <param name="connectionStringName">Name of the connection string name.</param>
        /// <exception cref="ConfigurationErrorsException">
        /// ConnectionString 'ProviderName' is not valid.
        /// </exception>
        public DbConnectionFactory(string connectionStringName)
        {
#if NFX
            ConnectionStringSettings setting = ConfigurationManager.ConnectionStrings[connectionStringName];

#if NET35
            if (StringEx.IsNullOrWhiteSpace(setting.ProviderName))
#else
            if (string.IsNullOrWhiteSpace(setting.ProviderName))
#endif
            {
                throw new ConfigurationErrorsException("ConnectionString 'ProviderName' is not valid.");
            }

            Use(setting.ProviderName, setting.ConnectionString); 
#endif
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DbConnectionFactory"/> class.
        /// </summary>
        /// <param name="providerName">Name of the provider.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <exception cref="ArgumentNullException">
        /// providerName
        /// or
        /// connectionString
        /// </exception>
        public DbConnectionFactory(string providerName,string connectionString)
        {
            Use(providerName, connectionString);
        }

        /// <summary>
        /// Use specialed providerName and connectionString.
        /// </summary>
        /// <param name="providerName"></param>
        /// <param name="connectionString"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public void Use(string providerName, string connectionString)
        {
#if NET35
            if (StringEx.IsNullOrWhiteSpace(providerName))
#else
            if (string.IsNullOrWhiteSpace(providerName))
#endif
            {
                throw new ArgumentNullException(nameof(providerName));
            }

#if NET35
            if (StringEx.IsNullOrWhiteSpace(connectionString))
#else
            if (string.IsNullOrWhiteSpace(connectionString))
#endif
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            connectionString = connectionString.Trim();

            string tmp = connectionString.ToLower();
            string conn = connectionString;
            
            if (tmp.Contains("provider"))
            {
                int index1 = tmp.IndexOf("provider", StringComparison.Ordinal);
                int index2 = tmp.IndexOf(";", StringComparison.Ordinal);

                if (index1 == 0)
                {
                    conn = connectionString.Substring(index2 + 1);
                }
                else
                {
                    string split = tmp.Substring(index1);
                    index2 = split.IndexOf(";", StringComparison.Ordinal);

                    if (index2 == split.Length - 1 || index2 == -1)
                    {
                        conn = connectionString.Substring(0, index1);
                    }
                    else
                    {
                        conn = connectionString.Substring(0, index1) + connectionString.Substring(index1 + index2 + 1);
                    }
                }
            }

            this.ConnectionString = conn;
            this.ProviderName = providerName;
#if NFX
            this.DbProviderFactory = DbProviderFactories.GetFactory(this.ProviderName);
#elif CFX
            this.DbProviderFactory = DbProviderFactories.GetFactory(this.ProviderName);
#endif
        }

        /// <summary>
        /// Get a new <see cref="T:IDbConnection"/>.
        /// </summary>
        /// <returns>The <see cref="IDbConnection"/>.</returns>
        public T New<T>() where T : DbConnection
        {
            IDbConnection conn = this.DbProviderFactory.CreateConnection();
            conn.ConnectionString = ConnectionString;

            return conn as T;
        }

        /// <summary>
        /// Get a new <see cref="DbConnection"/>.
        /// </summary>
        /// <returns></returns>
        public IDbConnection New()
        {
            IDbConnection conn = this.DbProviderFactory.CreateConnection();
            conn.ConnectionString = ConnectionString;

            return conn;
        }
    }
}
