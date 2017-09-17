using System;
using System.Collections.Generic;
using System.Data;
using Dapper;
#if NET35
using System.Linq;
#endif
#if NFX
using GodSharp.Data.Dapper.Extension;

#endif

// ReSharper disable PrivateFieldCanBeConvertedToLocalVariable
// ReSharper disable FieldCanBeMadeReadOnly.Local
// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace GodSharp.Data.Dapper
{
    /// <summary>
    /// Dapper DbContext
    /// </summary>
    /// <seealso cref="System.IDisposable" />
    public class DbContext : IDisposable
    {
        #region // Fileds        

        /// <summary>
        /// The database connection
        /// </summary>
        private IDbConnection _dbConnection;

        /// <summary>
        /// The database transaction
        /// </summary>
        private IDbTransaction _dbTransaction;

        /// <summary>
        /// The affected row number
        /// </summary>
        private int _affectedRowNumber;

        /// <summary>
        /// The has error
        /// </summary>
        private bool _hasError;

        /// <summary>
        /// The factory
        /// </summary>
        private DbConnectionFactory _factory;

        #endregion

        #region // Constructor methods

        /// <summary>
        /// Initializes a new instance of the <see cref="DbContext"/> class.
        /// </summary>
        protected DbContext()
        {
            _factory = new DbConnectionFactory();

            // ReSharper disable once VirtualMemberCallInConstructor
            OnConfiguration(_factory);

            if (_factory.DbProviderFactory != null)
            {
                _dbConnection = _factory.New();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DbContext"/> class.
        /// </summary>
        /// <param name="connectionStringName">Name of the connection string key.</param>
        protected DbContext(string connectionStringName)
        {
            _factory = new DbConnectionFactory(connectionStringName);

            _dbConnection = _factory.New();
        }

        #endregion

        #region // Custom methods

        /// <summary>
        /// Called when [configuration].
        /// </summary>
        /// <param name="factory">The factory.</param>
        protected virtual void OnConfiguration(DbConnectionFactory factory)
        {
        }

        /// <summary>
        /// Begins the transaction.
        /// </summary>
        /// <param name="il">The il.</param>
        protected void BeginTransaction(IsolationLevel? il = null)
        {
            if (_dbTransaction == null)
            {
				Open();

                _dbTransaction =
                    il == null ? _dbConnection.BeginTransaction() : _dbConnection.BeginTransaction(il.Value);
                _affectedRowNumber = 0;
            }
        }

        /// <summary>
        /// Commits transaction.
        /// </summary>
        /// <returns></returns>
        protected int Commit()
        {
            if (_dbTransaction != null)
            {
                if (_hasError)
                {
                    _dbTransaction.Rollback();
                }
                else
                {
                    _dbTransaction.Commit();
                }
            }

            int number = _affectedRowNumber;
            Reset();

            return number;
        }
        
        /// <summary>
        /// Callback transaction.
        /// </summary>
        protected void Callback()
        {
            if (_dbTransaction != null)
            {
				_dbTransaction?.Rollback();
				
				Reset();
            }
        }


        /// <summary>
        /// Opens the connection.
        /// </summary>
        /// <exception cref="Exception"></exception>
        protected void Open()
        {
            if (_dbConnection.State == ConnectionState.Closed)
            {
                try
                {
                    _dbConnection.Open();
                }
                catch (Exception ex)
                {
                    throw new Exception(ex.Message, ex);
                }
            }
        }

        /// <summary>
        /// reset internal object.
        /// </summary>
        public void Reset()
        {
            _hasError = false;
            _dbTransaction = null;
            _affectedRowNumber = 0;
        }

        #endregion

        #region // Dapper methods

        #region Execute

        /// <summary>
        /// Execute parameterized SQL.
        /// </summary>
        /// <param name="command">The command to execute on this connection.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>The number of rows affected.</returns>
        protected int Execute(CommandDefinition command,bool withoutTranTransaction=false)
        {
            try
            {
                command = new CommandDefinition(command.CommandText, command.Parameters, withoutTranTransaction ? null : _dbTransaction,
                    command.CommandTimeout, command.CommandType, command.Flags);
                int rows = _dbConnection.Execute(command);
                _affectedRowNumber += rows;
                return rows;
            }
            catch (Exception ex)
            {
                _hasError = true;
                throw new Exception(ex.Message, ex);
            }
        }

        /// <summary>
        /// Execute parameterized SQL.
        /// </summary>
        /// <param name="sql">The SQL to execute for this query.</param>
        /// <param name="param">The parameters to use for this query.</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>The number of rows affected.</returns>
        protected int Execute(string sql, object param = null, int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            try
            {
                int rows = _dbConnection.Execute(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
                _affectedRowNumber += rows;
                return rows;
            }
            catch (Exception ex)
            {
                _hasError = true;
                throw new Exception(ex.Message, ex);
            }
        }

        #endregion

        #region ExecuteReader

        /// <summary>
        /// Execute parameterized SQL and return an <see cref="IDataReader"/>.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>An <see cref="IDataReader"/> that can be used to iterate over the results of the SQL query.</returns>
        /// <remarks>
        /// This is typically used when the results of a query are not processed by Dapper, for example, used to fill a <see cref="DataTable"/>
        /// or <see cref="T:DataSet"/>.
        /// </remarks>
        protected IDataReader ExecuteReader(CommandDefinition command,bool withoutTranTransaction=false)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, withoutTranTransaction ? null : _dbTransaction,
                command.CommandTimeout, command.CommandType, command.Flags);
            return _dbConnection.ExecuteReader(command);
        }

        /// <summary>
        /// Execute parameterized SQL and return an <see cref="IDataReader"/>.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <param name="commandBehavior">The <see cref="CommandBehavior"/> flags for this reader.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>An <see cref="IDataReader"/> that can be used to iterate over the results of the SQL query.</returns>
        /// <remarks>
        /// This is typically used when the results of a query are not processed by Dapper, for example, used to fill a <see cref="DataTable"/>
        /// or <see cref="T:DataSet"/>.
        /// </remarks>
        protected IDataReader ExecuteReader(CommandDefinition command, CommandBehavior commandBehavior,bool withoutTranTransaction=false)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, withoutTranTransaction ? null : _dbTransaction,
                command.CommandTimeout, command.CommandType, command.Flags);
            return _dbConnection.ExecuteReader(command, commandBehavior);
        }

        /// <summary>
        /// Execute parameterized SQL and return an <see cref="IDataReader"/>.
        /// </summary>
        /// <param name="sql">The SQL to execute.</param>
        /// <param name="param">The parameters to use for this command.</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>An <see cref="IDataReader"/> that can be used to iterate over the results of the SQL query.</returns>
        /// <remarks>
        /// This is typically used when the results of a query are not processed by Dapper, for example, used to fill a <see cref="DataTable"/>
        /// or <see cref="T:DataSet"/>.
        /// </remarks>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// DataTable table = new DataTable("MyTable");
        /// using (var reader = ExecuteReader(cnn, sql, param))
        /// {
        ///     table.Load(reader);
        /// }
        /// ]]>
        /// </code>
        /// </example>
        protected IDataReader ExecuteReader(string sql, object param = null, int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.ExecuteReader(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
        }

        #endregion

        #region ExecuteScalar

        /// <summary>
        /// Execute parameterized SQL that selects a single value.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>The first cell selected as <see cref="object"/>.</returns>
        protected object ExecuteScalar(CommandDefinition command,bool withoutTranTransaction=false)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, withoutTranTransaction ? null : _dbTransaction,
                command.CommandTimeout, command.CommandType, command.Flags);
            return _dbConnection.ExecuteScalar(command);
        }

        /// <summary>
        /// Execute parameterized SQL that selects a single value.
        /// </summary>
        /// <param name="sql">The SQL to execute.</param>
        /// <param name="param">The parameters to use for this command.</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>The first cell selected as <see cref="object"/>.</returns>
        protected object ExecuteScalar(string sql, object param = null, int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.ExecuteScalar(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
        }

        /// <summary>
        /// Execute parameterized SQL that selects a single value.
        /// </summary>
        /// <typeparam name="T">The type to return.</typeparam>
        /// <param name="command">The command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>The first cell selected as <typeparamref name="T"/>.</returns>
        protected T ExecuteScalar<T>(CommandDefinition command,bool withoutTranTransaction=false)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, withoutTranTransaction ? null : _dbTransaction,
                command.CommandTimeout, command.CommandType, command.Flags);
            return _dbConnection.ExecuteScalar<T>(command);
        }

        /// <summary>
        /// Execute parameterized SQL that selects a single value.
        /// </summary>
        /// <typeparam name="T">The type to return.</typeparam>
        /// <param name="sql">The SQL to execute.</param>
        /// <param name="param">The parameters to use for this command.</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>The first cell returned, as <typeparamref name="T"/>.</returns>
        protected T ExecuteScalar<T>(string sql, object param = null, int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.ExecuteScalar<T>(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
        }

        #endregion

        #region Query

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="TReturn"/>.
        /// </summary>
        /// <typeparam name="TReturn">The type of results to return.</typeparam>
        /// <param name="command">The command used to query on this connection.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>
        /// A sequence of data of <typeparamref name="TReturn"/>; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected IEnumerable<TReturn> Query<TReturn>(CommandDefinition command,bool withoutTranTransaction=false)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, withoutTranTransaction ? null : _dbTransaction,
                command.CommandTimeout, command.CommandType, command.Flags);
            return _dbConnection.Query<TReturn>(command);
        }

#if !NET35
        /// <summary>
        /// Return a sequence of dynamic objects with properties matching the columns.
        /// </summary>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="buffered">Whether to buffer the results in memory.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <remarks>Note: each row can be accessed via "dynamic", or by casting to an IDictionary&lt;string,object&gt;</remarks>
        protected IEnumerable<dynamic> Query(string sql, object param = null, bool buffered = true,
            int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.Query(sql, param, withoutTranTransaction ? null : _dbTransaction, buffered, commandTimeout, commandType);
        }
#endif

        /// <summary>
        /// Executes a single-row query, returning the data typed as <paramref name="type"/>.
        /// </summary>
        /// <param name="type">The type to return.</param>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="buffered">Whether to buffer results in memory.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <c>null</c>.</exception>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected IEnumerable<object> Query(Type type, string sql, object param = null, bool buffered = true,
            int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.Query(type, sql, param, withoutTranTransaction ? null : _dbTransaction, buffered, commandTimeout, commandType);
        }

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="buffered">Whether to buffer results in memory.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected IEnumerable<T> Query<T>(string sql, object param = null, bool buffered = true,
            int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.Query<T>(sql, param, withoutTranTransaction ? null : _dbTransaction, buffered, commandTimeout, commandType);
        }

#if !NET35 
        /// <summary>
        /// Perform a multi-mapping query with an arbitrary number of input types. 
        /// This returns a single type, combined from the raw types via <paramref name="map"/>.
        /// </summary>
        /// <typeparam name="TReturn">The combined type to return.</typeparam>
        /// <param name="sql">The SQL to execute for this query.</param>
        /// <param name="types">Array of types in the recordset.</param>
        /// <param name="map">The function to map row types to the return type.</param>
        /// <param name="param">The parameters to use for this query.</param>
        /// <param name="buffered">Whether to buffer the results in memory.</param>
        /// <param name="splitOn">The field we should split and read the second object from (default: "Id").</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        protected IEnumerable<TReturn> Query<TReturn>(string sql, Type[] types, Func<object[], TReturn> map,
            object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.Query(sql, types, map, param, withoutTranTransaction ? null : _dbTransaction, buffered, splitOn, commandTimeout,
                commandType);
        }
#endif

        /// <summary>
        /// Perform a multi-mapping query with 2 input types. 
        /// This returns a single type, combined from the raw types via <paramref name="map"/>.
        /// </summary>
        /// <typeparam name="TFirst">The first type in the recordset.</typeparam>
        /// <typeparam name="TSecond">The second type in the recordset.</typeparam>
        /// <typeparam name="TReturn">The combined type to return.</typeparam>
        /// <param name="sql">The SQL to execute for this query.</param>
        /// <param name="map">The function to map row types to the return type.</param>
        /// <param name="param">The parameters to use for this query.</param>
        /// <param name="buffered">Whether to buffer the results in memory.</param>
        /// <param name="splitOn">The field we should split and read the second object from (default: "Id").</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        protected IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(string sql, Func<TFirst, TSecond, TReturn> map,
            object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.Query(sql, map, param, withoutTranTransaction ? null : _dbTransaction, buffered, splitOn, commandTimeout, commandType);
        }

        /// <summary>
        /// Perform a multi-mapping query with 3 input types. 
        /// This returns a single type, combined from the raw types via <paramref name="map"/>.
        /// </summary>
        /// <typeparam name="TFirst">The first type in the recordset.</typeparam>
        /// <typeparam name="TSecond">The second type in the recordset.</typeparam>
        /// <typeparam name="TThird">The third type in the recordset.</typeparam>
        /// <typeparam name="TReturn">The combined type to return.</typeparam>
        /// <param name="sql">The SQL to execute for this query.</param>
        /// <param name="map">The function to map row types to the return type.</param>
        /// <param name="param">The parameters to use for this query.</param>
        /// <param name="buffered">Whether to buffer the results in memory.</param>
        /// <param name="splitOn">The field we should split and read the second object from (default: "Id").</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        protected IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(string sql,
            Func<TFirst, TSecond, TThird, TReturn> map, object param = null, bool buffered = true,
            string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.Query(sql, map, param, withoutTranTransaction ? null : _dbTransaction, buffered, splitOn, commandTimeout, commandType);
        }

        /// <summary>
        /// Perform a multi-mapping query with 4 input types. 
        /// This returns a single type, combined from the raw types via <paramref name="map"/>.
        /// </summary>
        /// <typeparam name="TFirst">The first type in the recordset.</typeparam>
        /// <typeparam name="TSecond">The second type in the recordset.</typeparam>
        /// <typeparam name="TThird">The third type in the recordset.</typeparam>
        /// <typeparam name="TFourth">The fourth type in the recordset.</typeparam>
        /// <typeparam name="TReturn">The combined type to return.</typeparam>
        /// <param name="sql">The SQL to execute for this query.</param>
        /// <param name="map">The function to map row types to the return type.</param>
        /// <param name="param">The parameters to use for this query.</param>
        /// <param name="buffered">Whether to buffer the results in memory.</param>
        /// <param name="splitOn">The field we should split and read the second object from (default: "Id").</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        protected IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TReturn>(string sql,
            Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, bool buffered = true,
            string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.Query(sql, map, param, withoutTranTransaction ? null : _dbTransaction, buffered, splitOn, commandTimeout, commandType);
        }

#if !NET35
        /// <summary>
        /// Perform a multi-mapping query with 5 input types. 
        /// This returns a single type, combined from the raw types via <paramref name="map"/>.
        /// </summary>
        /// <typeparam name="TFirst">The first type in the recordset.</typeparam>
        /// <typeparam name="TSecond">The second type in the recordset.</typeparam>
        /// <typeparam name="TThird">The third type in the recordset.</typeparam>
        /// <typeparam name="TFourth">The fourth type in the recordset.</typeparam>
        /// <typeparam name="TFifth">The fifth type in the recordset.</typeparam>
        /// <typeparam name="TReturn">The combined type to return.</typeparam>
        /// <param name="sql">The SQL to execute for this query.</param>
        /// <param name="map">The function to map row types to the return type.</param>
        /// <param name="param">The parameters to use for this query.</param>
        /// <param name="buffered">Whether to buffer the results in memory.</param>
        /// <param name="splitOn">The field we should split and read the second object from (default: "Id").</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        protected IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(string sql,
            Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, object param = null, bool buffered = true,
            string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.Query(sql, map, param, withoutTranTransaction ? null : _dbTransaction, buffered, splitOn, commandTimeout, commandType);
        }

        /// <summary>
        /// Perform a multi-mapping query with 6 input types. 
        /// This returns a single type, combined from the raw types via <paramref name="map"/>.
        /// </summary>
        /// <typeparam name="TFirst">The first type in the recordset.</typeparam>
        /// <typeparam name="TSecond">The second type in the recordset.</typeparam>
        /// <typeparam name="TThird">The third type in the recordset.</typeparam>
        /// <typeparam name="TFourth">The fourth type in the recordset.</typeparam>
        /// <typeparam name="TFifth">The fifth type in the recordset.</typeparam>
        /// <typeparam name="TSixth">The sixth type in the recordset.</typeparam>
        /// <typeparam name="TReturn">The combined type to return.</typeparam>
        /// <param name="sql">The SQL to execute for this query.</param>
        /// <param name="map">The function to map row types to the return type.</param>
        /// <param name="param">The parameters to use for this query.</param>
        /// <param name="buffered">Whether to buffer the results in memory.</param>
        /// <param name="splitOn">The field we should split and read the second object from (default: "Id").</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        protected IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(string sql,
            Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, object param = null,
            bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.Query(sql, map, param, withoutTranTransaction ? null : _dbTransaction, buffered, splitOn, commandTimeout, commandType);
        }

        /// <summary>
        /// Perform a multi-mapping query with 7 input types. 
        /// This returns a single type, combined from the raw types via <paramref name="map"/>.
        /// </summary>
        /// <typeparam name="TFirst">The first type in the recordset.</typeparam>
        /// <typeparam name="TSecond">The second type in the recordset.</typeparam>
        /// <typeparam name="TThird">The third type in the recordset.</typeparam>
        /// <typeparam name="TFourth">The fourth type in the recordset.</typeparam>
        /// <typeparam name="TFifth">The fifth type in the recordset.</typeparam>
        /// <typeparam name="TSixth">The sixth type in the recordset.</typeparam>
        /// <typeparam name="TSeventh">The seventh type in the recordset.</typeparam>
        /// <typeparam name="TReturn">The combined type to return.</typeparam>
        /// <param name="sql">The SQL to execute for this query.</param>
        /// <param name="map">The function to map row types to the return type.</param>
        /// <param name="param">The parameters to use for this query.</param>
        /// <param name="buffered">Whether to buffer the results in memory.</param>
        /// <param name="splitOn">The field we should split and read the second object from (default: "Id").</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        protected IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(
            string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map,
            object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.Query(sql, map, param, withoutTranTransaction ? null : _dbTransaction, buffered, splitOn, commandTimeout, commandType);
        }
#endif

#if WF
        /// <summary>
        /// This returns a <see cref="DataTable"/>.
        /// </summary>
        /// <param name="sql">The SQL to execute for this query.</param>
        /// <param name="param">The parameters to use for this query.</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>A <see cref="DataTable"/>.</returns>
        protected DataTable Query(string sql, object param = null, int? commandTimeout =
            null, CommandType? commandType = default(CommandType?),bool withoutTranTransaction=false)
        {
            return _dbConnection.Query(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
        }
#endif

        #endregion

        #region QueryFirst

#if !NET35 
        /// <summary>
        /// Return a dynamic object with properties matching the columns.
        /// </summary>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <remarks>Note: the row can be accessed via "dynamic", or by casting to an IDictionary&lt;string,object&gt;</remarks>
        protected dynamic QueryFirst(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
            return _dbConnection.QueryFirst(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
        }
#endif

        /// <summary>
        /// Executes a single-row query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of result to return.</typeparam>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected T QueryFirst<T>(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
#if NET35
            return _dbConnection.Query<T>(sql, param, withoutTranTransaction ? null : _dbTransaction, false, commandTimeout, commandType).First();
#else
            return _dbConnection.QueryFirst<T>(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
#endif
        }

        /// <summary>
        /// Executes a single-row query, returning the data typed as <paramref name="type"/>.
        /// </summary>
        /// <param name="type">The type to return.</param>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <c>null</c>.</exception>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected object QueryFirst(Type type, string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
#if NET35
            return _dbConnection.Query(type, sql, param, withoutTranTransaction ? null : _dbTransaction, false, commandTimeout, commandType).First();
#else
            return _dbConnection.QueryFirst(type, sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
#endif
        }

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="command">The command used to query on this connection.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>
        /// A single instance or null of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected T QueryFirst<T>(CommandDefinition command,bool withoutTranTransaction=false)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, withoutTranTransaction ? null : _dbTransaction,
                command.CommandTimeout, command.CommandType, command.Flags);
#if NET35
            return _dbConnection.Query<T>(command).First();
#else
            return _dbConnection.QueryFirst<T>(command);
#endif
        }

        #endregion

        #region QueryFirstOrDefault

#if !NET35 
        /// <summary>
        /// Return a dynamic object with properties matching the columns.
        /// </summary>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <remarks>Note: the row can be accessed via "dynamic", or by casting to an IDictionary&lt;string,object&gt;</remarks>
        protected dynamic QueryFirstOrDefault(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
            return _dbConnection.QueryFirstOrDefault(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
        }
#endif

        /// <summary>
        /// Executes a single-row query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of result to return.</typeparam>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected T QueryFirstOrDefault<T>(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
#if NET35
            return _dbConnection.Query<T>(sql, param, withoutTranTransaction ? null : _dbTransaction, false, commandTimeout, commandType)
                .FirstOrDefault();
#else
            return _dbConnection.QueryFirstOrDefault<T>(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
#endif
        }

        /// <summary>
        /// Executes a single-row query, returning the data typed as <paramref name="type"/>.
        /// </summary>
        /// <param name="type">The type to return.</param>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <c>null</c>.</exception>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected object QueryFirstOrDefault(Type type, string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
#if NET35
            return _dbConnection.Query(type, sql, param, withoutTranTransaction ? null : _dbTransaction, false, commandTimeout, commandType)
                .FirstOrDefault();
#else
            return _dbConnection.QueryFirstOrDefault(type, sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
#endif
        }

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="command">The command used to query on this connection.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>
        /// A single or null instance of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected T QueryFirstOrDefault<T>(CommandDefinition command,bool withoutTranTransaction=false)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, withoutTranTransaction ? null : _dbTransaction,
                command.CommandTimeout, command.CommandType, command.Flags);
#if NET35
            return _dbConnection.Query<T>(command).FirstOrDefault();
#else
            return _dbConnection.QueryFirstOrDefault<T>(command);
#endif
        }

        #endregion

        #region QueryMultiple

        /// <summary>
        /// Execute a command that returns multiple result sets, and access each in turn.
        /// </summary>
        /// <param name="sql">The SQL to execute for this query.</param>
        /// <param name="param">The parameters to use for this query.</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        protected SqlMapper.GridReader QueryMultiple(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
            return _dbConnection.QueryMultiple(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
        }

        /// <summary>
        /// Execute a command that returns multiple result sets, and access each in turn.
        /// </summary>
        /// <param name="command">The command used to query on this connection.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        protected SqlMapper.GridReader QueryMultiple(CommandDefinition command,bool withoutTranTransaction=false)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, withoutTranTransaction ? null : _dbTransaction,
                command.CommandTimeout, command.CommandType, command.Flags);
            return _dbConnection.QueryMultiple(command);
        }

        #endregion

        #region QuerySingle

#if !NET35 
        /// <summary>
        /// Return a dynamic object with properties matching the columns.
        /// </summary>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <remarks>Note: the row can be accessed via "dynamic", or by casting to an IDictionary&lt;string,object&gt;</remarks>
        protected dynamic QuerySingle(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
            return _dbConnection.QuerySingle(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
        }
#endif

        /// <summary>
        /// Executes a single-row query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of result to return.</typeparam>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected T QuerySingle<T>(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
#if NET35
            return _dbConnection.Query<T>(sql, param, withoutTranTransaction ? null : _dbTransaction, false, commandTimeout, commandType).Single();
#else
            return _dbConnection.QuerySingle<T>(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
#endif
        }

        /// <summary>
        /// Executes a single-row query, returning the data typed as <paramref name="type"/>.
        /// </summary>
        /// <param name="type">The type to return.</param>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <c>null</c>.</exception>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected object QuerySingle(Type type, string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
#if NET35
            return _dbConnection.Query(type, sql, param, withoutTranTransaction ? null : _dbTransaction, false, commandTimeout, commandType).Single();
#else
            return _dbConnection.QuerySingle(type, sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
#endif
        }

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="command">The command used to query on this connection.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>
        /// A single instance of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected T QuerySingle<T>(CommandDefinition command,bool withoutTranTransaction=false)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, withoutTranTransaction ? null : _dbTransaction,
                command.CommandTimeout, command.CommandType, command.Flags);
#if NET35
            return _dbConnection.Query<T>(command).Single();
#else
            return _dbConnection.QuerySingle<T>(command);
#endif
        }

        #endregion

        #region QuerySingleOrDefault

#if !NET35 
        /// <summary>
        /// Return a dynamic object with properties matching the columns.
        /// </summary>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <remarks>Note: the row can be accessed via "dynamic", or by casting to an IDictionary&lt;string,object&gt;</remarks>
        protected dynamic QuerySingleOrDefault(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
            return _dbConnection.QuerySingleOrDefault(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
        }
#endif

        /// <summary>
        /// Executes a single-row query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of result to return.</typeparam>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected T QuerySingleOrDefault<T>(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
#if NET35
            return _dbConnection.Query<T>(sql, param, withoutTranTransaction ? null : _dbTransaction, false, commandTimeout, commandType)
                .SingleOrDefault();
#else
            return _dbConnection.QuerySingleOrDefault<T>(sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
#endif
        }

        /// <summary>
        /// Executes a single-row query, returning the data typed as <paramref name="type"/>.
        /// </summary>
        /// <param name="type">The type to return.</param>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <c>null</c>.</exception>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected object QuerySingleOrDefault(Type type, string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null,bool withoutTranTransaction=false)
        {
#if NET35
            return _dbConnection.Query(type, sql, param, withoutTranTransaction ? null : _dbTransaction, false, commandTimeout, commandType)
                .SingleOrDefault();
#else
            return _dbConnection.QuerySingleOrDefault(type, sql, param, withoutTranTransaction ? null : _dbTransaction, commandTimeout, commandType);
#endif
        }

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="command">The command used to query on this connection.</param>
        /// <param name="withoutTranTransaction">Whether to use a transaction.</param>
        /// <returns>
        /// A single instance of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        protected T QuerySingleOrDefault<T>(CommandDefinition command,bool withoutTranTransaction=false)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, withoutTranTransaction ? null : _dbTransaction,
                command.CommandTimeout, command.CommandType, command.Flags);
#if NET35
            return _dbConnection.Query<T>(command).SingleOrDefault();
#else
            return _dbConnection.QuerySingleOrDefault<T>(command);
#endif
        }

        #endregion

        #endregion

        #region IDisposable Support

        // ReSharper disable once InconsistentNaming
        private bool disposedValue; // To detect redundant calls

        /// <summary>
        /// Dispose
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _dbTransaction?.Dispose();
                    _dbConnection?.Dispose();
                }

                disposedValue = true;
            }
        }

        /// <summary>
        /// Dispose
        /// </summary>
        ~DbContext()
        {
            Dispose(false);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}