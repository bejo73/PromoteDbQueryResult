using System;
using System.Data.Common;

namespace BizTalkComponents.DbUtils
{
    public class DbQuery : IDbQuery
    {
        private string dbProvider;
        private string dbConnectionString;

        public DbQuery(string dbProvider, string dbConnectionString)
        {
            this.dbProvider = dbProvider;
            this.dbConnectionString = dbConnectionString;
        }

        public int ExecuteScalar(string query)
        {
            int result = 0;
            using (DbConnection connection = CreateDbConnection())
            {
                result = ExecuteDbCommand(connection, query);
            }
            return result;
        }

        private DbConnection CreateDbConnection()
        {
            DbConnection connection = null;

            if (this.dbConnectionString != null)
            {
                try
                {
                    DbProviderFactory factory = 
                        DbProviderFactories.GetFactory(this.dbProvider);

                    connection = factory.CreateConnection();
                    connection.ConnectionString = this.dbConnectionString;
                }
                catch (Exception e)
                {
                    connection = null;
                    throw e;
                }
            }

            return connection;
        }

        private int ExecuteDbCommand(DbConnection connection, string query)
        {
            int result = 0;

            try
            {
                connection.Open();

                DbCommand command = connection.CreateCommand();
                command.CommandText = query;

                result = (int)command.ExecuteScalar();
            }
            catch (Exception e)
            {
                throw e;
            }

            return result;
        }

    }
}
