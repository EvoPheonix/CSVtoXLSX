using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System.Data;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using Text = DocumentFormat.OpenXml.Spreadsheet.Text;

var startTime = DateTime.Now;
// TODO: add configurable varables to appsettings/config
var csvFilePath = "";
string connectionString = "";

// Prefix for the fileshare location
var matches = Regex.Match(csvFilePath, "([^\\\\]*)\\.[^.]*$");
var pathPrefix = $"";

var connectorRows = new List<ConnectorRow>();
var collectionLimitList = new List<CollectionLimit>();

// batching and looping variables for the document table
var batchingDone = false;
var index = 0;
int total;
int count;

// spreadsheet header
var headerRow = new Row() { RowIndex = 1 };
// TODO: add to config
var header = new List<string>() { "Original Source", "File Share Location", "Common Field For Version Grouping", "Modified", "ModifiedBy", "Created", "CreatedBy", "Major Version", "Minor Version", "Version Comments" };
// Initialize spreadsheet headers with know columns
foreach (var headerCell in header)
{
    headerRow.Append(new Cell() { InlineString = new InlineString() { Text = new Text(headerCell) } });
}


// TODO: change to better logging solution
Console.WriteLine("Filling Database");
ResetDatabase(connectionString);
FillSQLDatabase(csvFilePath, connectionString);

// TODO: make this a function to get all documents
Console.WriteLine("Getting Documents From Database");
var documents = GetAllDocuments(connectionString);
var padlDocs = GetAllPadl(connectionString);
var poDocs = GetAllPO(connectionString);
var invoiceDocs = GetAllInvoice(connectionString);
var promotionTenure = GetAllPromotionTenure(connectionString);

total = documents.Rows.Count;
count = total;

Console.WriteLine("Building Connector Rows");
Console.Clear();
while (!batchingDone)
{
    // Batching is to resolve a sql pool error
    var batch = documents.Select().Skip(index).Take(25000);

    Parallel.ForEach(batch, document =>
    {
        var pairing = new DocumentPairing();
        pairing.Document = document;
        pairing.Collections = new List<DataRow>();
        pairing.Versions = GetVersions(document["handle"].ToString(), connectionString);
        pairing.Collections.AddRange(GetCollection(document["Source_containment"].ToString(), connectionString));

        lock (collectionLimitList)
        {
            var collectionHandle = Regex.Matches(document["Source_containment"].ToString(), "[a-zA-Z][^,|;]*")[0].Value.Replace("\\", "").Replace("\"", "");
            var collectionItem = collectionLimitList.FirstOrDefault(x => x.CollectionHandle == collectionHandle);
            if (collectionItem == null)
            {

                collectionLimitList.Add(new CollectionLimit { CollectionHandle = collectionHandle, CollectionName = GetCollectionTitleByHandle(document["Source_containment"].ToString(), connectionString), itemCount = 1 });
            }
            else
            {
                collectionItem.itemCount++;
            }

        }

        for (var i = 0; i < pairing.Versions.Rows.Count; i++)
        {
            var version = pairing.Versions.Rows[i];
            var connectorRow = new ConnectorRow();
            connectorRow.CreatedBy = GetUserNameByID(pairing.Document["Destination_owner"].ToString(), connectionString);
            connectorRow.Created = ConvertToEST(version["create_date"].ToString()); 
            connectorRow.ModifiedBy = GetUserNameByID(version["Destination_modifiedBy"].ToString() == string.Empty ? pairing.Document["Destination_owner"].ToString() : version["Destination_modifiedBy"].ToString(), connectionString);
            connectorRow.Modified = ConvertToEST(version["modified_date"].ToString());
            connectorRow.FileShareLocation = pathPrefix + version["Destination_preferredRendition"].ToString() + "_0";
            // fix to use title and append file extension
            connectorRow.OriginalSource = "\\" + BuildPath(pairing.Collections, VerifyName(pairing.Versions.Rows.Count > 1 ? version["title"].ToString() : pairing.Document["title"].ToString(), pairing.Document["original_file_name"].ToString()), "title", collectionLimitList).Replace("\"", "");
            connectorRow.MajorVersion = version["version_number"].ToString();
            connectorRow.MinorVersion = "0";
            connectorRow.VersionComment = version["revision_comments"].ToString();
            connectorRow.CommonFieldForVersionGrouping = pairing.Document["handle"].ToString();
            connectorRow.CustomFields = BuildCustomFields(pairing.Document, connectionString, headerRow);

            // Lock to make thread safe so we are not missing rows
            lock (connectorRows)
            {
                connectorRows.Add(connectorRow);

            }
        }

        count--;
        Console.SetCursorPosition(0, 0);
        Console.WriteLine($"Working on {csvFilePath}");
        Console.WriteLine($"Remaining: {count}/{total}");
    });

    index += 25000;
    if (index > total)
    {
        batchingDone = true;
    }
}

count = padlDocs.Rows.Count;
foreach (DataRow document in padlDocs.Rows)
{
    var pairing = new DocumentPairing();
    pairing.Document = document;
    pairing.Collections = new List<DataRow>();
    pairing.Versions = GetVersions(document["handle"].ToString(), connectionString);
    pairing.Collections.AddRange(GetCollection(document["Source_containment"].ToString(), connectionString));

    for (var i = 0; i < pairing.Versions.Rows.Count; i++)
    {
        var version = pairing.Versions.Rows[i];
        var connectorRow = new ConnectorRow();
        connectorRow.CreatedBy = GetUserNameByID(pairing.Document["Destination_owner"].ToString(), connectionString);
        connectorRow.Created = ConvertToEST(version["create_date"].ToString()); 
        connectorRow.ModifiedBy = GetUserNameByID(version["Destination_modifiedBy"].ToString() == string.Empty ? pairing.Document["Destination_owner"].ToString() : version["Destination_modifiedBy"].ToString(), connectionString);
        connectorRow.Modified = ConvertToEST(version["modified_date"].ToString());
        connectorRow.FileShareLocation = pathPrefix + version["Destination_preferredRendition"].ToString() + "_0";
        // fix to use title and append file extension
        connectorRow.OriginalSource = "\\" + BuildPath(pairing.Collections, VerifyName(pairing.Versions.Rows.Count > 1 ? version["title"].ToString() : pairing.Document["title"].ToString(), pairing.Document["original_file_name"].ToString()), "title", collectionLimitList).Replace("\"", "");
        connectorRow.MajorVersion = version["version_number"].ToString();
        connectorRow.MinorVersion = "0";
        connectorRow.VersionComment = version["revision_comments"].ToString();
        connectorRow.CommonFieldForVersionGrouping = pairing.Document["handle"].ToString();
        connectorRow.CustomFields = BuildCustomFields(pairing.Document, connectionString, headerRow);

        connectorRows.Add(connectorRow);
    }

    count--;
    Console.SetCursorPosition(0, 0);
    Console.WriteLine($"Working on {csvFilePath}");
    Console.WriteLine($"Remaining: {count}/{padlDocs.Rows.Count}");
}

count = poDocs.Rows.Count;
foreach (DataRow document in poDocs.Rows)
{
    var pairing = new DocumentPairing();
    pairing.Document = document;
    pairing.Collections = new List<DataRow>();
    pairing.Versions = GetVersions(document["handle"].ToString(), connectionString);
    pairing.Collections.AddRange(GetCollection(document["Source_containment"].ToString(), connectionString));

    for (var i = 0; i < pairing.Versions.Rows.Count; i++)
    {
        var version = pairing.Versions.Rows[i];
        var connectorRow = new ConnectorRow();
        connectorRow.CreatedBy = GetUserNameByID(pairing.Document["Destination_owner"].ToString(), connectionString);
        connectorRow.Created = ConvertToEST(version["create_date"].ToString()); 
        connectorRow.ModifiedBy = GetUserNameByID(version["Destination_modifiedBy"].ToString() == string.Empty ? pairing.Document["Destination_owner"].ToString() : version["Destination_modifiedBy"].ToString(), connectionString);
        connectorRow.Modified = ConvertToEST(version["modified_date"].ToString());
        connectorRow.FileShareLocation = pathPrefix + version["Destination_preferredRendition"].ToString() + "_0";
        // fix to use title and append file extension
        connectorRow.OriginalSource = "\\" + BuildPath(pairing.Collections, VerifyName(pairing.Versions.Rows.Count > 1 ? version["title"].ToString() : pairing.Document["title"].ToString(), pairing.Document["original_file_name"].ToString()), "title", collectionLimitList).Replace("\"", "");
        connectorRow.MajorVersion = version["version_number"].ToString();
        connectorRow.MinorVersion = "0";
        connectorRow.VersionComment = version["revision_comments"].ToString();
        connectorRow.CommonFieldForVersionGrouping = pairing.Document["handle"].ToString();
        connectorRow.CustomFields = BuildCustomFields(pairing.Document, connectionString, headerRow);

        connectorRows.Add(connectorRow);
    }

    count--;
    Console.SetCursorPosition(0, 0);
    Console.WriteLine($"Working on {csvFilePath}");
    Console.WriteLine($"Remaining: {count}/{poDocs.Rows.Count}");
}

count = poDocs.Rows.Count;
foreach (DataRow document in invoiceDocs.Rows)
{
    var pairing = new DocumentPairing();
    pairing.Document = document;
    pairing.Collections = new List<DataRow>();
    pairing.Versions = GetVersions(document["handle"].ToString(), connectionString);
    pairing.Collections.AddRange(GetCollection(document["Source_containment"].ToString(), connectionString));

    for (var i = 0; i < pairing.Versions.Rows.Count; i++)
    {
        var version = pairing.Versions.Rows[i];
        var connectorRow = new ConnectorRow();
        connectorRow.CreatedBy = GetUserNameByID(pairing.Document["Destination_owner"].ToString(), connectionString);
        connectorRow.Created = ConvertToEST(version["create_date"].ToString());
        connectorRow.ModifiedBy = GetUserNameByID(version["Destination_modifiedBy"].ToString() == string.Empty ? pairing.Document["Destination_owner"].ToString() : version["Destination_modifiedBy"].ToString(), connectionString);
        connectorRow.Modified = ConvertToEST(version["modified_date"].ToString());
        connectorRow.FileShareLocation = pathPrefix + version["Destination_preferredRendition"].ToString() + "_0";
        // fix to use title and append file extension
        connectorRow.OriginalSource = "\\" + BuildPath(pairing.Collections, VerifyName(pairing.Versions.Rows.Count > 1 ? version["title"].ToString() : pairing.Document["title"].ToString(), pairing.Document["original_file_name"].ToString()), "title", collectionLimitList).Replace("\"", "");
        connectorRow.MajorVersion = version["version_number"].ToString();
        connectorRow.MinorVersion = "0";
        connectorRow.VersionComment = version["revision_comments"].ToString();
        connectorRow.CommonFieldForVersionGrouping = pairing.Document["handle"].ToString();
        connectorRow.CustomFields = BuildCustomFields(pairing.Document, connectionString, headerRow);

        connectorRows.Add(connectorRow);
    }

    count--;
    Console.SetCursorPosition(0, 0);
    Console.WriteLine($"Working on {csvFilePath}");
    Console.WriteLine($"Remaining: {count}/{invoiceDocs.Rows.Count}");
}

count = promotionTenure.Rows.Count;
foreach (DataRow document in promotionTenure.Rows)
{
    var pairing = new DocumentPairing();
    pairing.Document = document;
    pairing.Collections = new List<DataRow>();
    pairing.Versions = GetVersions(document["handle"].ToString(), connectionString);
    pairing.Collections.AddRange(GetCollection(document["Source_containment"].ToString(), connectionString));

    for (var i = 0; i < pairing.Versions.Rows.Count; i++)
    {
        var version = pairing.Versions.Rows[i];
        var connectorRow = new ConnectorRow();
        connectorRow.CreatedBy = GetUserNameByID(pairing.Document["Destination_owner"].ToString(), connectionString);
        connectorRow.Created = ConvertToEST(version["create_date"].ToString()); //DateTime.ParseExact(Regex.Replace(version["create_date"].ToString(), @" (EST|EDT)", ""), "ddd MMM dd HH:mm:ss yyyy", System.Globalization.CultureInfo.InvariantCulture);
        connectorRow.ModifiedBy = GetUserNameByID(version["Destination_modifiedBy"].ToString() == string.Empty ? pairing.Document["Destination_owner"].ToString() : version["Destination_modifiedBy"].ToString(), connectionString);
        connectorRow.Modified = ConvertToEST(version["modified_date"].ToString());// DateTime.ParseExact(Regex.Replace(version["modified_date"].ToString(), @" (EST|EDT)", ""), "ddd MMM dd HH:mm:ss yyyy", System.Globalization.CultureInfo.InvariantCulture);
        connectorRow.FileShareLocation = pathPrefix + version["Destination_preferredRendition"].ToString() + "_0";
        // fix to use title and append file extension
        connectorRow.OriginalSource = "\\" + BuildPath(pairing.Collections, VerifyName(pairing.Versions.Rows.Count > 1 ? version["title"].ToString() : pairing.Document["title"].ToString(), pairing.Document["original_file_name"].ToString()), "title", collectionLimitList).Replace("\"", "");
        connectorRow.MajorVersion = version["version_number"].ToString();
        connectorRow.MinorVersion = "0";
        connectorRow.VersionComment = version["revision_comments"].ToString();
        connectorRow.CommonFieldForVersionGrouping = pairing.Document["handle"].ToString();
        connectorRow.CustomFields = BuildCustomFields(pairing.Document, connectionString, headerRow);

        connectorRows.Add(connectorRow);
    }

    count--;
    Console.SetCursorPosition(0, 0);
    Console.WriteLine($"Working on {csvFilePath}");
    Console.WriteLine($"Remaining: {count}/{promotionTenure.Rows.Count}");
}

Console.WriteLine("\rBuilding Spreadsheet");
BuildTHEMISSpreadsheet(connectorRows, matches.Groups[1].Value, csvFilePath, collectionLimitList, headerRow);

Console.WriteLine("Done");
Console.WriteLine($"Time Lapsed: {DateTime.Now - startTime}");

// TODO fix the broken spreadsheet export, current workaround is to open in excel manually and repair
// Puts each connector row into the spreadsheet
static void BuildTHEMISSpreadsheet(List<ConnectorRow> connectorRows, string collectionID, string csvFilePath, List<CollectionLimit> collectionLimits, Row headerRow)
{
    var limitCollections = collectionLimits.Where(x => x.itemCount > 4500).Select(y => $"{y.CollectionName}").ToList();

    SpreadsheetDocument spreadsheetDocument = SpreadsheetDocument.Create($"{collectionID}-CMFE.xlsx", SpreadsheetDocumentType.Workbook);

    WorkbookPart workbookpart = spreadsheetDocument.AddWorkbookPart();
    workbookpart.Workbook = new Workbook();

    WorksheetPart worksheetPart = workbookpart.AddNewPart<WorksheetPart>();
    worksheetPart.Worksheet = new Worksheet(new SheetData());

    Sheets sheets = spreadsheetDocument.WorkbookPart.Workbook.AppendChild<Sheets>(new Sheets());

    Sheet sheet = new Sheet()
    {
        Id = spreadsheetDocument.WorkbookPart.GetIdOfPart(worksheetPart),
        SheetId = 1,
        Name = "CMFE",
    };
    sheets.Append(sheet);

    Worksheet worksheet = worksheetPart.Worksheet;
    SheetData sheetData = worksheet.GetFirstChild<SheetData>();

    sheetData.Append(headerRow);

    var cmfeTotal = connectorRows.Count;
    var cmfeCount = cmfeTotal;

    foreach (var connectorRow in connectorRows.OrderBy(x => x.CommonFieldForVersionGrouping).ThenBy(y => y.MajorVersion))
    {
        Row lastRow = sheetData.Elements<Row>().LastOrDefault();
        var rowToAdd = new Row() { RowIndex = ((lastRow?.RowIndex ?? 0) + 1) };

        var collectionName = limitCollections.FirstOrDefault(x => connectorRow.OriginalSource.Contains(x));

        if (collectionName != null)
        {
            // Done as a workaround to memory issues, this just renames the first batch of the view limit of 5k in sharepoint
            connectorRow.OriginalSource = Regex.Replace(connectorRow.OriginalSource, $"{collectionName}\\\\(?!{collectionName})", $"{collectionName}\\{collectionName} - 1\\");
        }

        rowToAdd.Append(new Cell[]
        {
            new Cell() { InlineString = new InlineString(){ Text = new Text(Regex.Replace(Regex.Replace(connectorRow.OriginalSource, "\\t", " "), "(\\.\\\\\\.)|(\\.\\\\)|(\\\\\\.)", "")) } },
            new Cell() { InlineString = new InlineString(){ Text = new Text(Regex.Replace(Regex.Replace(connectorRow.FileShareLocation, "\\t", " "), "(\\.\\\\\\.)|(\\.\\\\)|(\\\\\\.)", "")) } },
            new Cell() { InlineString = new InlineString(){ Text = new Text(connectorRow.CommonFieldForVersionGrouping) } },
            new Cell() { InlineString = new InlineString(){ Text = new Text(connectorRow.Modified.ToString()) } },
            new Cell() { InlineString = new InlineString(){ Text = new Text(connectorRow.ModifiedBy) } },
            new Cell() { InlineString = new InlineString(){ Text = new Text(connectorRow.Created.ToString()) } },
            new Cell() { InlineString = new InlineString() { Text = new Text(connectorRow.CreatedBy) }},
            new Cell() { InlineString = new InlineString(){ Text = new Text(connectorRow.MajorVersion) }},
            new Cell() { InlineString = new InlineString() { Text = new Text(connectorRow.MinorVersion) }},
            new Cell() { InlineString = new InlineString() { Text = new Text(connectorRow.VersionComment) }}
        });

        foreach (var cell in headerRow.Skip(10))
        {
            rowToAdd.Append(new Cell() { InlineString = new InlineString() { Text = new Text(Regex.Replace(Regex.Replace(connectorRow.CustomFields.Where(x => x.Key == cell.InnerText).FirstOrDefault().Value?.ToString() ?? string.Empty, "\\t", " "), "(\\.\\\\\\.)|(\\.\\\\)|(\\\\\\.)", "")) } });
        }

        sheetData.InsertAfter(rowToAdd, lastRow);

        cmfeCount--;
        Console.SetCursorPosition(0, 0);
        Console.WriteLine($"Working on {csvFilePath}remaining");
        Console.WriteLine($"CMFE Remaining: {cmfeCount}/{cmfeTotal}");
    }

    spreadsheetDocument.Dispose();
}

// Deletes and recreated the database for each run of the script
static void ResetDatabase(string connectionString)
{
    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        connection.Open();
        var sqlCommand = new SqlCommand("use master; alter database CSVtoXLXS set single_user with rollback immediate; drop database CSVtoXLXS;", connection);
        sqlCommand.ExecuteNonQuery();

        sqlCommand = new SqlCommand("Create DATABASE CSVtoXLXS", connection);
        sqlCommand.ExecuteNonQuery();

        connection.Close();
        connection.Dispose();
    }
}

// Fills the database with every "table" found in the csv file
static void FillSQLDatabase(string csvFilePath, string connectionString)
{
    List<DataTable> dataTables = new List<DataTable>();
    // ingest the csv file before insertion
    using (var csvReader = new StreamReader(csvFilePath))
    {
        while (!csvReader.EndOfStream)
        {
            // you cannot have a table called User or Group in SQL, also works around other issues with the export
            var tableName = csvReader.ReadLine().Replace(" ", "").Replace(",", "").Replace("User", "Users").Replace("Group", "Groups");
            // unused and problematic tables
            string[] ignoreTables = { "MailMessage" };
            if (!tableName.Contains(",") && tableName.Length < 128 && tableName.Length != 0 && !ignoreTables.Contains(tableName))
            {
                DataTable csvData = new DataTable(tableName);
                // Read the header line and create the table columns
                string[] header = csvReader.ReadLine().Split(',').Where(x => x != string.Empty).ToArray();
                foreach (string line in header)
                {
                    csvData.Columns.Add(line);
                }

                // Read the remaining lines and populate the table rows
                string[] fields = { };
                do
                {
                    // TODO: clean this up
                    var sansCommaLine = Regex.Replace(Regex.Replace(Regex.Replace(csvReader.ReadLine(), ",(?!(?:[^\"]*\"[^\"]*\")*[^\"]*$)", ";"), "\\t", " "), "(\\.\\\\\\.)|(\\.\\\\)|(\\\\\\.)", "");

                    fields = sansCommaLine.Split(',');
                    if (!string.IsNullOrWhiteSpace(fields[0]))
                    {
                        fields = fields.Select(x => x.Replace(";", ",").Substring(0, Math.Min(x.Length, 3999))).ToArray();
                        csvData.Rows.Add(fields.Take(csvData.Columns.Count).ToArray());
                    }

                } while (!string.IsNullOrWhiteSpace(fields[0]) && !csvReader.EndOfStream);

                dataTables.Add(csvData);
            }
        }
    }

    // Insert the data into SQL Server using SqlBulkCopy
    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        connection.Open();
        foreach (var table in dataTables)
        {
            bool tableExists;
            string tableName = table.TableName;

            using (SqlCommand command = new SqlCommand($"SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}'", connection))
            {
                tableExists = (int)command.ExecuteScalar() > 0;
            }

            if (!tableExists)
            {
                string columnDefinition = GenerateColumnDefinition(table);
                string createTableQuery = $"CREATE TABLE {tableName} ({columnDefinition})";


                using (SqlCommand command = new SqlCommand(createTableQuery, connection))
                {
                    // Execute the SQL query
                    command.ExecuteNonQuery();
                }
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.WriteToServer(table);
            }
        }
        connection.Close();
        connection.Dispose();
    }
}

static string GenerateColumnDefinition(DataTable dataTable)
{
    string columnDefinition = "";

    // Iterate over the DataTable's columns
    foreach (DataColumn column in dataTable.Columns)
    {
        string columnName = column.ColumnName;
        string columnType = GetSqlType(column.DataType);

        columnDefinition += $"{columnName} {columnType}, ";
    }

    // Remove the trailing comma and space
    columnDefinition = columnDefinition.TrimEnd(',', ' ');

    return columnDefinition;
}

// TODO: make this less useless
static string GetSqlType(Type dataType)
{
    // Map the .NET data types to SQL Server data types
    if (dataType == typeof(int))
    {
        return "INT";
    }
    else if (dataType == typeof(string))
    {
        return "NVARCHAR(4000)";
    }
    else if (dataType == typeof(DateTime))
    {
        return "DATE";
    }

    // Handle other data types as needed

    return "nvarchar(4000)";
}

// Gets all entries from the documents table
static DataTable GetAllDocuments(string connectionString)
{
    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        try
        {
            SqlCommand command = new SqlCommand($"SELECT * FROM Document", connection);
            SqlDataAdapter adapter = new SqlDataAdapter(command);
            DataTable dataTable = new DataTable();
            adapter.Fill(dataTable);

            connection.Close();
            connection.Dispose();

            return dataTable;
        }
        catch
        {
            return new DataTable();
        }
    }
}

// some exports have this table, we do not know why. This table is another documents table
static DataTable GetAllPromotionTenure(string connectionString)
{
    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        SqlCommand command = new SqlCommand($"SELECT * FROM ProfessorTenure", connection);
        SqlDataAdapter adapter = new SqlDataAdapter(command);
        DataTable dataTable = new DataTable();
        try
        {
            adapter.Fill(dataTable);
        }
        catch (Exception ex) { }

        connection.Close();
        connection.Dispose();

        return dataTable;
    }
}

// some exports have this table, we do not know why. This table is another documents table
static DataTable GetAllPadl(string connectionString)
{
    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        SqlCommand command = new SqlCommand($"SELECT * FROM padl_finan", connection);
        SqlDataAdapter adapter = new SqlDataAdapter(command);
        DataTable dataTable = new DataTable();
        try
        {
            adapter.Fill(dataTable);
        }
        catch (Exception ex) { }

        connection.Close();
        connection.Dispose();

        return dataTable;
    }
}

// some exports have this table, we do not know why. This table is another documents table
static DataTable GetAllPO(string connectionString)
{
    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        SqlCommand command = new SqlCommand($"SELECT * FROM PO", connection);
        SqlDataAdapter adapter = new SqlDataAdapter(command);
        DataTable dataTable = new DataTable();
        try
        {
            adapter.Fill(dataTable);
        }
        catch (Exception ex) { }

        connection.Close();
        connection.Dispose();

        return dataTable;
    }
}

// some exports have this table, we do not know why. This table is another documents table
static DataTable GetAllInvoice(string connectionString)
{
    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        SqlCommand command = new SqlCommand($"SELECT * FROM Invoice", connection);
        SqlDataAdapter adapter = new SqlDataAdapter(command);
        DataTable dataTable = new DataTable();
        try
        {
            adapter.Fill(dataTable);
        }
        catch (Exception ex) { }

        connection.Close();
        connection.Dispose();

        return dataTable;
    }
}

// TODO: clean this function
// This will get all the versions for a document
static DataTable GetVersions(string DocName, string connectionString)
{
    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        try
        {
            SqlCommand command = new SqlCommand($"SELECT * FROM Version WHERE Source_version='{DocName}' order by version_number", connection);
            SqlDataAdapter adapter = new SqlDataAdapter(command);
            DataTable dataTable = new DataTable();
            adapter.Fill(dataTable);

            // some versions do not have a source version so we need to find it with part of the preferrred rendition
            command = new SqlCommand($"SELECT * FROM Version WHERE Destination_preferredRendition like '%{DocName}' order by version_number", connection);
            adapter = new SqlDataAdapter(command);
            adapter.Fill(dataTable);

            connection.Close();
            connection.Dispose();

            return dataTable;
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            connection.Close();
            connection.Dispose();
            return null;
        }
    }
}

// TODO: review this function
// This gets the collections leading to a file
static List<DataRow> GetCollection(string CollectionName, string connectionString)
{
    var CollectionMatches = Regex.Matches(CollectionName, "[a-zA-Z][^,|;]*");
    List<DataRow> Collections = new List<DataRow>();
    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        try
        {
            SqlCommand command = new SqlCommand($"SELECT * FROM Collection where handle='{CollectionMatches[0].Value.Replace("\\", "").Replace("\"", "")}'", connection);
            SqlDataAdapter adapter = new SqlDataAdapter(command);
            DataTable collection = new DataTable();
            adapter.Fill(collection);

            if (collection.Rows.Count == 0)
            {
                Collections.AddRange(GetCollection(CollectionMatches[1].Value, connectionString));
            }
            else
            {

                bool hasParent = !string.IsNullOrWhiteSpace(collection.Rows[0]["Source_containment"].ToString());

                if (hasParent)
                {
                    Collections.AddRange(GetCollection(collection.Rows[0]["Source_containment"].ToString(), connectionString));
                }

                Collections.Add(collection.Rows[0]);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        connection.Close();
        connection.Dispose();
    }
    return Collections;
}

// TODO: review this function
static string GetUserNameByID(string userID, string connectionString)
{
    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        try
        {
            SqlCommand command = new SqlCommand($"SELECT email FROM Users where handle='{userID}'", connection);
            SqlDataAdapter adapter = new SqlDataAdapter(command);
            DataTable dataTable = new DataTable();
            adapter.Fill(dataTable);

            if (dataTable.Rows.Count == 0)
            {
                return userID;
            }

            connection.Close();
            connection.Dispose();

            return dataTable.Rows[0].ItemArray.FirstOrDefault().ToString().Replace("\"", "");
        }
        catch
        {
            // waiting fixes the sql error
            Thread.Sleep(1000);
            SqlCommand command = new SqlCommand($"SELECT email FROM Users where handle='{userID}'", connection);
            SqlDataAdapter adapter = new SqlDataAdapter(command);
            DataTable dataTable = new DataTable();
            adapter.Fill(dataTable);

            if (dataTable.Rows.Count == 0)
            {
                return userID;
            }

            connection.Close();
            connection.Dispose();

            return dataTable.Rows[0].ItemArray.FirstOrDefault().ToString().Replace("\"", "");
        }
    }
}

// TODO: review this function
static string GetCollectionTitleByHandle(string handle, string connectionString)
{
    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        var matches = Regex.Matches(handle, "[a-zA-Z][^,|;]*");
        SqlCommand command = new SqlCommand($"SELECT title FROM Collection WHERE handle='{matches[0].Value.Replace("\\", "").Replace("\"", "")}'", connection);
        SqlDataAdapter adapter = new SqlDataAdapter(command);
        DataTable dataTable = new DataTable();
        adapter.Fill(dataTable);

        if (dataTable.Rows.Count == 0)
        {
            // some files have two source collections but always only one of them exists, this is to check for the second collection
            command = new SqlCommand($"SELECT title FROM Collection WHERE handle='{matches[1].Value.Replace("\\", "").Replace("\"", "")}'", connection);
            adapter = new SqlDataAdapter(command);
            dataTable = new DataTable();
            adapter.Fill(dataTable);
        }

        connection.Close();
        connection.Dispose();

        return dataTable.Select()[0]["title"].ToString();
    }
}

// Used to convert EDT Time to EST time
static DateTime ConvertToEST(string date)
{
    DateTime timeToConvert = DateTime.ParseExact(Regex.Replace(date, @" (EST|EDT)", ""), "ddd MMM dd HH:mm:ss yyyy", System.Globalization.CultureInfo.InvariantCulture);

    if (date.Contains("EDT"))
    {
        timeToConvert = timeToConvert.AddHours(-1);
    }

    return timeToConvert;
}

// TODO: review this function
// builds the original source path
static string BuildPath(List<DataRow> Collections, string docName, string pathType, List<CollectionLimit> collectionLimits)
{
    var path = string.Empty;
    var last = Collections.LastOrDefault();

    foreach (DataRow collection in Collections)
    {
        int? collectionTotal;
        lock (collectionLimits)
        {
            collectionTotal = collectionLimits.Find(x => x.CollectionHandle == collection["handle"].ToString())?.itemCount;
        }
        // removes illegal characters
        var collectionName = Regex.Replace(collection[pathType].ToString(), "~|#|%|&|\\*|{|}|\\\\|:|<|>|\\?|\\/|\\||\"", "");

        if (collectionTotal > 4500)
        {
            // workaround for sharepoint 5K limit on views
            collectionName += $"\\{collectionName} - {(int)collectionTotal / 4500 + 1}";
        }

        path += collectionName + "\\";
    }
    // removes illegal characters
    path += Regex.Replace(docName, "~|#|%|&|\\*|{|}|\\\\|:|<|>|\\?|\\/|\\||\"", "").Replace("..", ".");

    // If the path is too long rebuild using the collection id instead of title
    if (path.Length > 1000/*460*/)
    {
        path = BuildPath(Collections, docName, "handle", collectionLimits);
    }

    return path;
}

// sometimes files dont have an extension so we take it from another field
static string VerifyName(string title, string originalTitle)
{
    var match = Regex.Match(title, @"\..*$");

    if (match.Success)
    {
        return title;
    }
    else
    {
        var extension = Regex.Match(originalTitle, @"\..*$").Value;
        return title += extension;
    }
}

// adds custom metadata
static Dictionary<string, string> BuildCustomFields(DataRow document, string connectionString, Row headerRow)
{
    Dictionary<string, string> customFields = new Dictionary<string, string>();
    var NonCustomFieldNames = new string[] { "handle", "highest_version_used", "create_date", "max_versions", "modified_date", "Destination_owner", "Destination_modifiedBy", "title", "original_file_name" };
    var columns = document.Table.Columns;

    for (var i = 0; i < columns.Count; i++)
    {
        if (!NonCustomFieldNames.Contains(columns[i].ToString()))
        {
            var key = columns[i].ToString();
            if (document.ItemArray[i].ToString().Contains("User-"))
            {
                customFields.Add(key, GetUserNameByID(document.ItemArray[i]?.ToString(), connectionString));
            }
            else
            {
                customFields.Add(key, document.ItemArray[i]?.ToString());
            }

            lock (headerRow)
            {
                if (headerRow.ChildElements.Where(x => x.InnerText == key).Count() == 0)
                {

                    headerRow.Append(new Cell() { InlineString = new InlineString() { Text = new Text(key) } });

                }
            }
        }
    }

    return customFields;
}

// A class used to create xlsx rows
public class ConnectorRow
{
    public string OriginalSource { get; set; }
    public string FileShareLocation { get; set; }
    public string CommonFieldForVersionGrouping { get; set; }
    public DateTime Modified { get; set; }
    public string ModifiedBy { get; set; }
    public DateTime Created { get; set; }
    public string CreatedBy { get; set; }
    public string MajorVersion { get; set; }
    public string MinorVersion { get; set; }
    public string VersionComment { get; set; }
    public Dictionary<string, string> CustomFields { get; set; }
}

// class to pair documents with versions and collections
public struct DocumentPairing
{
    public DataRow Document { get; set; }
    public DataTable Versions { get; set; }
    public DataTable URLs { get; set; }
    public List<DataRow> Collections { get; set; }
}

// class to use for working around sharepoint views 5k limit 
public class CollectionLimit
{
    public string CollectionHandle { get; set; }
    public string CollectionName { get; set; }
    public int itemCount { get; set; }
}