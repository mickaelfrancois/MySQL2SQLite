// Authors:
//  Mickaël FRANCOIS <forum@fpc-france.com>
//
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

using MySql.Data.MySqlClient;
using NDesk.Options;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mysql2Sqlite
{
    class Program
    {
        /// <summary>
        /// MySQL connectin
        /// </summary>
        private MySqlConnection mysqlCnx = null;

        /// <summary>
        /// SQLite connection
        /// </summary>
        private SQLiteConnection sqliteCnx = null;
                
        private static string sqliteFile = "";
        private static string sqlitePassword = "";
        private static string mysqlUser = "";
        private static string mysqlPassword = "";
        private static string mysqlDatabase = "";
        private static string mysqlHost = "";
        private static bool schemaOnly = false;
        private static bool logQuery = false;


        static void Main( string[] args )
        {
            bool show_help = args.Length==0;

            var p = new OptionSet() {
            { "sqliteFile=", "Full path of the SQLite database", v => sqliteFile = v },
            { "sqlitePassword=", "Full path of the SQLite database", v => sqlitePassword = v },
            { "mysqlUser=", "MySQL user", v => mysqlUser = v },
            { "mysqlPassword=", "MySQL password", v => mysqlPassword = v },
            { "mysqlDatabase=", "Name of the MySQL database", v => mysqlDatabase = v },
            { "mysqlHost=", "Address of the MySQL server", v => mysqlHost = v },
            { "schemaOnly=", "1 if you want to generate only schema", v => schemaOnly = v=="1" },          
            { "logQuery=", "1 if you want to log queries", v => logQuery = v=="1" },  
            { "h|help",  "show this message and exit",  v => show_help = v != null },        
            };

            p.Parse( args );


            if( show_help )
            {
                ShowHelp( p );
                return;
            }
            try
            {                
               
                Program instance = new Program();
                instance.Start();
            }
            catch( OptionException e )
            {
                Console.WriteLine( e.Message );
                Console.WriteLine( "Try `mysql2sqlite --help' for more information." );
                return;
            }


        }


        static void ShowHelp( OptionSet p )
        {
            Console.WriteLine( "Usage: mysql2sqlite [OPTIONS]" );
            Console.WriteLine( "Migrate a MySQL database to SQLite database." );
            Console.WriteLine();
            Console.WriteLine( "Options:" );
            p.WriteOptionDescriptions( Console.Out );
        }

        private void Start()
        {
            this.OpenMySql();
            this.OpenSQLite();

            this.CreateSchema();
            if( !schemaOnly )
                this.GenerateDatas();

            this.CloseMySql();
            this.CloseSQLite();
        }


        #region Open / Close connections

        /// <summary>
        /// Open MySql connection
        /// </summary>
        private void OpenMySql()
        {
            string connectionString = string.Format( "server={0}; user id={1}; password={2}; database={3}", mysqlHost, mysqlUser, mysqlPassword, mysqlDatabase );

            this.mysqlCnx = new MySqlConnection( connectionString );
            this.mysqlCnx.Open();
        }


        /// <summary>
        /// Open Sqlite connection
        /// </summary>
        private void OpenSQLite()
        {
            if( System.IO.File.Exists( sqliteFile ) )
                System.IO.File.Delete( sqliteFile );

            string connectionString = string.Format( "Data Source={0};Version=3;Password={1};New=True;", sqliteFile, sqlitePassword );

            this.sqliteCnx = new SQLiteConnection( connectionString );
            this.sqliteCnx.Open();
        }


        /// <summary>
        /// Close MySql connection
        /// </summary>
        private void CloseMySql()
        {
            this.mysqlCnx.Close();
        }


        /// <summary>
        /// Close SQLite connection
        /// </summary>
        private void CloseSQLite()
        {
            this.sqliteCnx.Close();
        }

        #endregion



        #region Schema

        /// <summary>
        /// Get list of tables of MySql
        /// </summary>
        /// <returns></returns>
        private DataTable ShowTables()
        {
            DataTable table;
            using( var command = this.mysqlCnx.CreateCommand() )
            {
                command.CommandText = "SHOW TABLES";
                table = new DataTable();
                table.Load( command.ExecuteReader() );
            }
            return table;
        }


        /// <summary>
        /// Get fields of a table
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private DataTable ShowFields( string tableName )
        {
            DataTable table;
            using( var command = this.mysqlCnx.CreateCommand() )
            {
                command.CommandText = string.Format( "SHOW FIELDS FROM {0}", tableName );
                table = new DataTable();
                table.Load( command.ExecuteReader() );
            }
            return table;
        }


        /// <summary>
        /// Get index of a table
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private DataTable ShowIndex( string tableName )
        {
            DataTable table;
            using( var command = this.mysqlCnx.CreateCommand() )
            {
                command.CommandText = string.Format( "SHOW INDEX FROM {0}", tableName );
                table = new DataTable();
                table.Load( command.ExecuteReader() );
            }
            return table;
        }



        /// <summary>
        /// Get primary key of a table
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private DataTable ShowPrimaryKey( string tableName )
        {
            DataTable table;
            using( var command = this.mysqlCnx.CreateCommand() )
            {
                command.CommandText = string.Format( "SHOW INDEX FROM {0} WHERE key_name = 'PRIMARY'", tableName );
                table = new DataTable();
                table.Load( command.ExecuteReader() );
            }
            return table;
        }


        /// <summary>
        /// Create SQLite shema
        /// </summary>
        private void CreateSchema()
        {
            DataTable tableMySql = this.ShowTables();

            foreach( DataRow rowTable in tableMySql.Rows )
            {
                string tableName = rowTable[ 0 ].ToString();


                this.CreateTable( tableName );
                this.CreateIndex( tableName );

            }
        }



        /// <summary>
        /// Create a table
        /// </summary>
        /// <param name="tableName"></param>
        private void CreateTable( string tableName )
        {
            DataTable fieldsMySQL = this.ShowFields( tableName );
            DataTable primaryFields = this.ShowPrimaryKey( tableName );

            StringBuilder createTableQuery = new StringBuilder();
            createTableQuery.Append( string.Format( "CREATE TABLE {0}", tableName ) );
            createTableQuery.Append( " ( " );

            string autoincrement = "";
            foreach( DataRow rowFields in fieldsMySQL.Rows )
            {
                string field = rowFields[ "field" ].ToString();
                string type = rowFields[ "type" ].ToString();
                type = type.Replace( "unsigned", "" ).Trim();
                string isNull = rowFields[ "Null" ].ToString() == "YES" ? "NULL" : "";
                string key = rowFields[ "Key" ].ToString();
                string extra = rowFields[ "extra" ].ToString();
                string defaultValue = rowFields[ "default" ].ToString();
                autoincrement = ( extra == "auto_increment" ? "AUTOINCREMENT" : "" );
                defaultValue = ( !string.IsNullOrEmpty( defaultValue ) ? string.Format( "DEFAULT '{0}'", defaultValue ) : "" );
                createTableQuery.Append( string.Format( "{0} {1} {2} {3}, ", field, type, isNull, defaultValue ) );

            }

            string sep = "";
            createTableQuery.Append( " PRIMARY KEY (" );
            foreach( DataRow rowPrimaryKey in primaryFields.Rows )
            {
                string field = rowPrimaryKey[ "column_name" ].ToString();
                createTableQuery.Append( string.Format( "{0} {1} {2}", sep, field, autoincrement ) );
            }
            createTableQuery.Append( " )" );


            createTableQuery.Append( " ) " );


            var command = this.sqliteCnx.CreateCommand();
            command.CommandText = createTableQuery.ToString();

            if( logQuery )
                this.WriteConsole( command.CommandText );

            try
            {
                command.ExecuteNonQuery();
            }
            catch( Exception ex )
            {
                this.WriteError( ex.Message );
            }

        }


        /// <summary>
        /// Create all index of a table
        /// </summary>
        /// <param name="tableName"></param>
        private void CreateIndex( string tableName )
        {
            DataTable indexMySQL = this.ShowIndex( tableName );
            string sep = "";

            DataTable listIndexName = indexMySQL.AsEnumerable().GroupBy( r => r.Field<string>( "Key_Name" ) ).Select( g => g.First() ).CopyToDataTable();
            foreach( DataRow rowIndexName in listIndexName.Rows )
            {
                string indexName = rowIndexName[ "key_name" ].ToString();

                if( indexName == "PRIMARY" )
                    continue;

                StringBuilder creatIndexQuery = new StringBuilder();

                DataRow[] index = indexMySQL.Select( string.Format( "key_name LIKE '{0}'", indexName ), "seq_in_index ASC" );
                for( int i = 0; i < index.Length; i++ )
                {
                    DataRow rowIndex = index[ i ];

                    if( i == 0 )
                    {
                        int non_unique = Int32.Parse( rowIndex[ "non_unique" ].ToString() );
                        string unique = ( non_unique == 0 ? "UNIQUE" : "" );
                        creatIndexQuery.Append( string.Format( "CREATE {0} INDEX {1}{2} ON {1} ( ", unique, tableName, indexName ) );
                        sep = "";
                    }


                    string field = rowIndex[ "column_name" ].ToString();

                    creatIndexQuery.Append( string.Format( "{0} {1} ", sep, field ) );
                    sep = ", ";
                }
                creatIndexQuery.Append( " ) " );

                var command = this.sqliteCnx.CreateCommand();
                command.CommandText = creatIndexQuery.ToString();

                if( logQuery )
                    this.WriteConsole( command.CommandText );

                try
                {
                    command.ExecuteNonQuery();
                }
                catch( Exception ex )
                {
                    this.WriteError( ex.Message );
                }
            }
        }


        #endregion


        #region Datas

        /// <summary>
        /// Fill SQLite table with MySQL datas
        /// </summary>
        private void GenerateDatas()
        {
            DataTable tableMySql = this.ShowTables();


            // Read all tables
            foreach( DataRow rowTable in tableMySql.Rows )
            {
                string tableName = rowTable[ 0 ].ToString();

                StringBuilder insertQuery = new StringBuilder();
                StringBuilder insertValuesQuery = new StringBuilder();
                insertQuery.Append( String.Format( "INSERT INTO {0} (", tableName ) );
                insertValuesQuery.Append( " VALUES (" );

                DataTable fieldsMySQL = this.ShowFields( tableName );

                string sep = "";
                var insertCommand = this.sqliteCnx.CreateCommand();

                // Create insert command
                foreach( DataRow rowFields in fieldsMySQL.Rows )
                {
                    string field = rowFields[ "field" ].ToString();

                    insertQuery.Append( sep + field );
                    insertValuesQuery.Append( sep + "@" + field );

                    insertCommand.Parameters.Add( new SQLiteParameter( "@" + field ) );
                    sep = ",";
                }
                insertQuery.Append( " ) " );
                insertValuesQuery.Append( " ) " );
                insertCommand.CommandText = insertQuery.ToString() + insertValuesQuery.ToString();


                // Start a transaction to speed up queries
                var transaction = this.sqliteCnx.BeginTransaction();

                // Read MySQL table datas
                var selectCommand = this.mysqlCnx.CreateCommand();
                selectCommand.CommandText = string.Format( "SELECT * FROM {0}", tableName );
                using( var reader = selectCommand.ExecuteReader() )
                {
                    while( reader.Read() )
                    {
                        foreach( DataRow rowFields in fieldsMySQL.Rows )
                        {
                            string field = rowFields[ "field" ].ToString();
                            insertCommand.Parameters[ "@" + field ].Value = reader[ field ];
                        }

                        if( logQuery )
                            this.WriteConsole( insertCommand.CommandText );

                        try
                        {
                            insertCommand.ExecuteNonQuery();
                        }
                        catch( Exception ex )
                        {
                            this.WriteError( ex.Message );
                        }
                    }
                }

                transaction.Commit();
            }

        }

        #endregion


        #region Log


        /// <summary>
        /// Write message to console
        /// </summary>
        /// <param name="message"></param>
        private void WriteConsole( string message )
        {
            Console.WriteLine( message );
        }


        /// <summary>
        /// Write error to console
        /// </summary>
        /// <param name="message"></param>
        private void WriteError( string message )
        {
            Console.Error.WriteLine( message );
        }

        #endregion
    }

}
