#if !NETSTANDARD1_3
using Dapper;
using System.Data;

namespace GodSharp.Data.Dapper.Extension
{
    /// <summary>
    /// IDbConnection Extension
    /// </summary>
    public static class DbConnectionExtension
    {
        /// <summary>
        /// Execute parameterized SQL and return an <see cref="DataTable"/>.
        /// </summary>
        /// <param name="cnn">The connection to query on.</param>
        /// <param name="sql">The SQL to execute.</param>
        /// <param name="param">The parameters to use for this command.</param>
        /// <param name="transaction">The transaction to use for this query.</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <returns>An <see cref="IDataReader"/> that can be used to iterate over the results of the SQL query.</returns>
        public static DataTable Query(this IDbConnection cnn, string sql, object param = null, IDbTransaction transaction = null,int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            IDataReader reader = cnn.ExecuteReader(sql, param, transaction, commandTimeout, commandType);

            if (reader == null)
            {
                return null;
            }

            DataTable dt = new DataTable();
            bool init = false;
            dt.BeginLoadData();
            object[] vals = new object[0];

            while (reader.Read())
            {
                if (!init)
                {
                    init = true;
                    int fieldCount = reader.FieldCount;
                    for (int i = 0; i < fieldCount; i++)
                    {
                        dt.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
                    }
                    vals = new object[fieldCount];
                }
                reader.GetValues(vals);
                dt.LoadDataRow(vals, true);
            }

            reader.Close();
            dt.EndLoadData();

            return dt;
        }
    }
}
#endif
