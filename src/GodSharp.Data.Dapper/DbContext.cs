﻿using System;
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
        public DbContext(string connectionStringName)
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
        public void BeginTransaction(IsolationLevel? il = null)
        {
            if (_dbTransaction == null)
            {
                _dbTransaction =
                    il == null ? _dbConnection.BeginTransaction() : _dbConnection.BeginTransaction(il.Value);
                _affectedRowNumber = 0;
            }
        }

        /// <summary>
        /// Commits the specified rollback.
        /// </summary>
        /// <param name="rollback">if set to <c>true</c> [rollback].</param>
        /// <returns></returns>
        public int Commit(bool rollback = false)
        {
            if (_dbTransaction != null)
            {
                if (_hasError || rollback)
                {
                    _dbTransaction.Rollback();
                }
                else
                {
                    _dbTransaction.Commit();
                }

                _hasError = false;
                _dbTransaction = null;
            }

            int number = _affectedRowNumber;
            _affectedRowNumber = 0;

            return number;
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
        /// <returns>The number of rows affected.</returns>
        public int Execute(CommandDefinition command)
        {
            try
            {
                command = new CommandDefinition(command.CommandText, command.Parameters, _dbTransaction,
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
        /// <returns>The number of rows affected.</returns>
        public int Execute(string sql, object param = null, int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?))
        {
            try
            {
                int rows = _dbConnection.Execute(sql, param, _dbTransaction, commandTimeout, commandType);
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
        /// <returns>An <see cref="IDataReader"/> that can be used to iterate over the results of the SQL query.</returns>
        /// <remarks>
        /// This is typically used when the results of a query are not processed by Dapper, for example, used to fill a <see cref="DataTable"/>
        /// or <see cref="T:DataSet"/>.
        /// </remarks>
        public IDataReader ExecuteReader(CommandDefinition command)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, _dbTransaction,
                command.CommandTimeout, command.CommandType, command.Flags);
            return _dbConnection.ExecuteReader(command);
        }

        /// <summary>
        /// Execute parameterized SQL and return an <see cref="IDataReader"/>.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <param name="commandBehavior">The <see cref="CommandBehavior"/> flags for this reader.</param>
        /// <returns>An <see cref="IDataReader"/> that can be used to iterate over the results of the SQL query.</returns>
        /// <remarks>
        /// This is typically used when the results of a query are not processed by Dapper, for example, used to fill a <see cref="DataTable"/>
        /// or <see cref="T:DataSet"/>.
        /// </remarks>
        public IDataReader ExecuteReader(CommandDefinition command, CommandBehavior commandBehavior)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, _dbTransaction,
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
        public IDataReader ExecuteReader(string sql, object param = null, int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.ExecuteReader(sql, param, _dbTransaction, commandTimeout, commandType);
        }

        #endregion

        #region ExecuteScalar

        /// <summary>
        /// Execute parameterized SQL that selects a single value.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <returns>The first cell selected as <see cref="object"/>.</returns>
        public object ExecuteScalar(CommandDefinition command)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, _dbTransaction,
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
        /// <returns>The first cell selected as <see cref="object"/>.</returns>
        public object ExecuteScalar(string sql, object param = null, int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.ExecuteScalar(sql, param, _dbTransaction, commandTimeout, commandType);
        }

        /// <summary>
        /// Execute parameterized SQL that selects a single value.
        /// </summary>
        /// <typeparam name="T">The type to return.</typeparam>
        /// <param name="command">The command to execute.</param>
        /// <returns>The first cell selected as <typeparamref name="T"/>.</returns>
        public T ExecuteScalar<T>(CommandDefinition command)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, _dbTransaction,
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
        /// <returns>The first cell returned, as <typeparamref name="T"/>.</returns>
        public T ExecuteScalar<T>(string sql, object param = null, int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.ExecuteScalar<T>(sql, param, _dbTransaction, commandTimeout, commandType);
        }

        #endregion

        #region Query

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="TReturn"/>.
        /// </summary>
        /// <typeparam name="TReturn">The type of results to return.</typeparam>
        /// <param name="command">The command used to query on this connection.</param>
        /// <returns>
        /// A sequence of data of <typeparamref name="TReturn"/>; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public IEnumerable<TReturn> Query<TReturn>(CommandDefinition command)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, _dbTransaction,
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
        /// <remarks>Note: each row can be accessed via "dynamic", or by casting to an IDictionary&lt;string,object&gt;</remarks>
        public IEnumerable<dynamic> Query(string sql, object param = null, bool buffered = true,
            int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.Query(sql, param, _dbTransaction, buffered, commandTimeout, commandType);
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
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <c>null</c>.</exception>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public IEnumerable<object> Query(Type type, string sql, object param = null, bool buffered = true,
            int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.Query(type, sql, param, _dbTransaction, buffered, commandTimeout, commandType);
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
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public IEnumerable<T> Query<T>(string sql, object param = null, bool buffered = true,
            int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.Query<T>(sql, param, _dbTransaction, buffered, commandTimeout, commandType);
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
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        public IEnumerable<TReturn> Query<TReturn>(string sql, Type[] types, Func<object[], TReturn> map,
            object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.Query(sql, types, map, param, _dbTransaction, buffered, splitOn, commandTimeout,
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
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        public IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(string sql, Func<TFirst, TSecond, TReturn> map,
            object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.Query(sql, map, param, _dbTransaction, buffered, splitOn, commandTimeout, commandType);
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
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(string sql,
            Func<TFirst, TSecond, TThird, TReturn> map, object param = null, bool buffered = true,
            string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.Query(sql, map, param, _dbTransaction, buffered, splitOn, commandTimeout, commandType);
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
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TReturn>(string sql,
            Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, bool buffered = true,
            string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.Query(sql, map, param, _dbTransaction, buffered, splitOn, commandTimeout, commandType);
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
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(string sql,
            Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, object param = null, bool buffered = true,
            string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.Query(sql, map, param, _dbTransaction, buffered, splitOn, commandTimeout, commandType);
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
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(string sql,
            Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, object param = null,
            bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.Query(sql, map, param, _dbTransaction, buffered, splitOn, commandTimeout, commandType);
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
        /// <returns>An enumerable of <typeparamref name="TReturn"/>.</returns>
        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(
            string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map,
            object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.Query(sql, map, param, _dbTransaction, buffered, splitOn, commandTimeout, commandType);
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
        /// <returns>A <see cref="DataTable"/>.</returns>
        public DataTable Query(string sql, object param = null, int? commandTimeout =
            null, CommandType? commandType = default(CommandType?))
        {
            return _dbConnection.Query(sql, param, _dbTransaction, commandTimeout, commandType);
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
        /// <remarks>Note: the row can be accessed via "dynamic", or by casting to an IDictionary&lt;string,object&gt;</remarks>
        public dynamic QueryFirst(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return _dbConnection.QueryFirst(sql, param, _dbTransaction, commandTimeout, commandType);
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
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public T QueryFirst<T>(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
#if NET35
            return _dbConnection.Query<T>(sql, param, _dbTransaction, false, commandTimeout, commandType).First();
#else
            return _dbConnection.QueryFirst<T>(sql, param, _dbTransaction, commandTimeout, commandType);
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
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <c>null</c>.</exception>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public object QueryFirst(Type type, string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
#if NET35
            return _dbConnection.Query(type, sql, param, _dbTransaction, false, commandTimeout, commandType).First();
#else
            return _dbConnection.QueryFirst(type, sql, param, _dbTransaction, commandTimeout, commandType);
#endif
        }

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="command">The command used to query on this connection.</param>
        /// <returns>
        /// A single instance or null of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public T QueryFirst<T>(CommandDefinition command)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, _dbTransaction,
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
        /// <remarks>Note: the row can be accessed via "dynamic", or by casting to an IDictionary&lt;string,object&gt;</remarks>
        public dynamic QueryFirstOrDefault(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return _dbConnection.QueryFirstOrDefault(sql, param, _dbTransaction, commandTimeout, commandType);
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
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public T QueryFirstOrDefault<T>(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
#if NET35
            return _dbConnection.Query<T>(sql, param, _dbTransaction, false, commandTimeout, commandType)
                .FirstOrDefault();
#else
            return _dbConnection.QueryFirstOrDefault<T>(sql, param, _dbTransaction, commandTimeout, commandType);
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
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <c>null</c>.</exception>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public object QueryFirstOrDefault(Type type, string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
#if NET35
            return _dbConnection.Query(type, sql, param, _dbTransaction, false, commandTimeout, commandType)
                .FirstOrDefault();
#else
            return _dbConnection.QueryFirstOrDefault(type, sql, param, _dbTransaction, commandTimeout, commandType);
#endif
        }

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="command">The command used to query on this connection.</param>
        /// <returns>
        /// A single or null instance of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public T QueryFirstOrDefault<T>(CommandDefinition command)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, _dbTransaction,
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
        public SqlMapper.GridReader QueryMultiple(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return _dbConnection.QueryMultiple(sql, param, _dbTransaction, commandTimeout, commandType);
        }

        /// <summary>
        /// Execute a command that returns multiple result sets, and access each in turn.
        /// </summary>
        /// <param name="command">The command used to query on this connection.</param>
        public SqlMapper.GridReader QueryMultiple(CommandDefinition command)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, _dbTransaction,
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
        /// <remarks>Note: the row can be accessed via "dynamic", or by casting to an IDictionary&lt;string,object&gt;</remarks>
        public dynamic QuerySingle(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return _dbConnection.QuerySingle(sql, param, _dbTransaction, commandTimeout, commandType);
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
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public T QuerySingle<T>(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
#if NET35
            return _dbConnection.Query<T>(sql, param, _dbTransaction, false, commandTimeout, commandType).Single();
#else
            return _dbConnection.QuerySingle<T>(sql, param, _dbTransaction, commandTimeout, commandType);
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
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <c>null</c>.</exception>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public object QuerySingle(Type type, string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
#if NET35
            return _dbConnection.Query(type, sql, param, _dbTransaction, false, commandTimeout, commandType).Single();
#else
            return _dbConnection.QuerySingle(type, sql, param, _dbTransaction, commandTimeout, commandType);
#endif
        }

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="command">The command used to query on this connection.</param>
        /// <returns>
        /// A single instance of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public T QuerySingle<T>(CommandDefinition command)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, _dbTransaction,
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
        /// <remarks>Note: the row can be accessed via "dynamic", or by casting to an IDictionary&lt;string,object&gt;</remarks>
        public dynamic QuerySingleOrDefault(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return _dbConnection.QuerySingleOrDefault(sql, param, _dbTransaction, commandTimeout, commandType);
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
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public T QuerySingleOrDefault<T>(string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
#if NET35
            return _dbConnection.Query<T>(sql, param, _dbTransaction, false, commandTimeout, commandType)
                .SingleOrDefault();
#else
            return _dbConnection.QuerySingleOrDefault<T>(sql, param, _dbTransaction, commandTimeout, commandType);
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
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <c>null</c>.</exception>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public object QuerySingleOrDefault(Type type, string sql, object param = null, int? commandTimeout = null,
            CommandType? commandType = null)
        {
#if NET35
            return _dbConnection.Query(type, sql, param, _dbTransaction, false, commandTimeout, commandType)
                .SingleOrDefault();
#else
            return _dbConnection.QuerySingleOrDefault(type, sql, param, _dbTransaction, commandTimeout, commandType);
#endif
        }

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="command">The command used to query on this connection.</param>
        /// <returns>
        /// A single instance of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public T QuerySingleOrDefault<T>(CommandDefinition command)
        {
            command = new CommandDefinition(command.CommandText, command.Parameters, _dbTransaction,
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