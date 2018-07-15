using System;
using System.Collections.Generic;
using System.Text;
using Quartz;
using Quartz.Impl;
using System.Threading.Tasks;// included along with Quartz 
using System.Xml;
using System.IO;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Web;
using System.Linq;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
using System.Text.RegularExpressions;


namespace IVRConsoleApp
{
    class Program
    {
        public static IConfiguration Configuration { get; set; }

        ////Create a new directory if not exists
        public static void CreateIfMissing(string path)
        {
            bool folderExists = Directory.Exists((path));
            if (!folderExists)
                Directory.CreateDirectory((path));
        }

        public static bool CheckIfDirExist(string path)
        {
            bool folderExists = Directory.Exists((path));
            if (folderExists)
                return true;
            else
                return false;
        }

        public static void Configurationsettings()
        {
            string environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            var builder = new ConfigurationBuilder()
                         .AddJsonFile($"tsconfig.{environment}.json", optional: true);

            Configuration = builder.Build();
        }

        static void Main(string[] args)
        {
            try
            {

                Configurationsettings();
                string logFilePath = Convert.ToString(Configuration["logFilePath"]);
                ////Create a new directory if not exists
                CreateIfMissing(logFilePath);

                Log.Logger = new LoggerConfiguration()
                            .MinimumLevel.Debug()
                            .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                            .Enrich.FromLogContext()
                            .WriteTo.Console()
                            .WriteTo.RollingFile(new CompactJsonFormatter(), logFilePath, shared: true)
                            .CreateLogger();

                Log.Information($"[{ DateTime.Now}]  IVR Console App Main Method started!");

                TaskIVRJob().GetAwaiter().GetResult();

                Log.Information($"[{ DateTime.Now}]  IVR Console App Main Method ended!");
                Console.ReadKey();
            }
            catch (System.Exception ex)
            {
                Log.Error("Error in main method" + ex.Message);
            }
            finally
            {
                

            }
        }

		/// <summary>
		/// Scheduling Task for a specific interval
		/// </summary>
		/// <returns></returns>
        public static async Task TaskIVRJob()
        {
            try
            {
                Log.Information($"[{ DateTime.Now}]  TaskIVRJob Task Method started!");

                IScheduler scheduler;

                var schedulerFactory = new StdSchedulerFactory();

                scheduler = schedulerFactory.GetScheduler().Result;

                scheduler.Start().Wait();
                // dynamic builder;
                Configurationsettings();

                int ScheduleIntervalInMinute = Convert.ToInt32(Configuration["ScheduleIntervalInMinute"]);
                JobKey jobKey = JobKey.Create("IVRJob");  
                IJobDetail job = JobBuilder.Create<IVRJob>().WithIdentity(jobKey).Build();
                ITrigger trigger = TriggerBuilder.Create()
                    .WithIdentity("JobTrigger")
                    .StartNow()
                    .WithSimpleSchedule(x => x.WithIntervalInMinutes(ScheduleIntervalInMinute).RepeatForever())
                    .Build();
                await scheduler.ScheduleJob(job, trigger);

                Log.Information($"[{ DateTime.Now}]  TaskIVRJob Task Method ended!");
            }
            catch (System.Exception ex)
            {
                Log.Error("Error in main method" + ex.Message);
            }
            finally
            {
               
            }

        }

        public class IVRJob : IJob
         {

            public IVRJob()
            {

            }

            public Task Execute(IJobExecutionContext context)
            {
                try
                {
                   
                    Log.Information($"[{ DateTime.Now}]  IVR Job Execute Method started!");

                    Configurationsettings();

                  

                    string TemplatePath = System.IO.Path.GetFullPath("FileTranslation.xml");

					string OutputXMLPath = Convert.ToString(Configuration["OutputRIPPath"]); 

                    string TempXMLPath = Convert.ToString(Configuration["XMLArchivePath"]) + "\\" + DateTime.Now.ToString("MM-dd-yyyy");

					string ArchiveStorageFolderPath = Convert.ToString(Configuration["CSVArchivePath"]) + "\\" + DateTime.Now.ToString("MM-dd-yyyy");

                    

                    //Create a new directory if not exists
                    CreateIfMissing(OutputXMLPath);
                    CreateIfMissing(TempXMLPath);
                   
                    string SystemId_Templatepath = System.IO.Path.GetFullPath("SystemId_Translation.xml");

                    XmlTextReader xmlreader = new XmlTextReader(SystemId_Templatepath);
                    //reading the xml data  
                    DataSet ds = new DataSet();
                    ds.ReadXml(xmlreader);
                    xmlreader.Close();

                   
                    DataTable dt_sourcepath = new DataTable();
                    dt_sourcepath = ds.Tables[3];



                    List<string> filenamespath = new List<string>();
                    bool filesfound;

                    for (int i = 0; i < dt_sourcepath.Columns.Count; i++)
                    {
                        string key = dt_sourcepath.Columns[i].ColumnName;
                        string value = dt_sourcepath.Rows[0][key.ToString()].ToString();
                        DirectoryInfo di = new DirectoryInfo(value);
                        if (CheckIfDirExist(value))
                        {
                            filesfound = false;
                            foreach (FileInfo fi in di.GetFiles())
                            {
                                if (Path.GetExtension(fi.FullName.ToString()).ToUpper() == ".CSV")
                                {
                                    filenamespath.Add(fi.FullName);
                                    filesfound = true;
                                    Log.Information($"[{ DateTime.Now}]  Filename path found:" + fi.FullName.ToString());
                                }

                            }
                            if (filesfound == false)
                            {
                                Log.Information("No CSV files found in path:" + value);
                            }
                           
                        }
                        else
                            Log.Information(value + ":Source folder doesn't exist");
                    }


                    foreach (string filepath in filenamespath)
                    {
                        FileInfo fi = new FileInfo(filepath);
                        DataTable dtcsv = new DataTable();
                        dtcsv = ReadCSVToDT(fi.FullName.ToString());


                        WriteXML(TemplatePath, TempXMLPath, OutputXMLPath, dtcsv, SystemId_Templatepath, fi.Name.ToString(), fi.FullName.ToString(), fi.DirectoryName.ToString(), ArchiveStorageFolderPath);
                    }

                    Log.Information($"[{ DateTime.Now}]  IVR Job Execute Method ended!");

                    //Console.WriteLine("Time:" + DateTime.Now.ToString("yyyy-dd--MM-hh-mm-ss-fff"));
                }
                catch (System.Exception ex)
                {
                    Log.Error("Error in Execute method" + ex.Message);
                }
                finally
                {
                   
                }
                return Task.FromResult(0);

            }
			/// <summary>
			/// Reading CSV file and converting to datatable
			/// </summary>
			/// <param name="csvfilepath"></param>
			/// <returns></returns>
            public DataTable ReadCSVToDT(string csvfilepath)
            {
                DataTable dt = new DataTable();
                try
                {
                    Log.Information($"[{ DateTime.Now}]  ReadCSVToDT Method started!");
                    Log.Information("csvfilepath:" + csvfilepath);


                    using (StreamReader sr = new StreamReader(csvfilepath))
                    {

                        string[] headers = sr.ReadLine().Split(',');
                        //DataTable dt = new DataTable();
                        foreach (string header in headers)
                        {
                            dt.Columns.Add(header);
                        }
                        while (!sr.EndOfStream)
                        {
                            string[] rows = Regex.Split(sr.ReadLine(), ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                            DataRow dr = dt.NewRow();
                            for (int i = 0; i < headers.Length; i++)
                            {
                                dr[i] = rows[i];
                            }
                            dt.Rows.Add(dr);
                        }

                    }

                    Log.Information($"[{ DateTime.Now}]  ReadCSVToDT Method ended!");
                }
                catch (System.Exception ex)
                {
                    Log.Error("Error in ReadCSVToDT method" + ex.Message);
                }
                finally
                {
                    //Log.CloseAndFlush();
                }
                return dt;
            }

/// <summary>
/// Writing XML file to destination path
/// </summary>
/// <param name="templatepath"></param>
/// <param name="TempXMLPath"></param>
/// <param name="outputxmlpath"></param>
/// <param name="datatablecsv"></param>
/// <param name="SystemId_Templatepath"></param>
/// <param name="filename"></param>
/// <param name="filefullname"></param>
/// <param name="filedirname"></param>
/// <param name="ArchiveStorageFolderPath"></param>
            public void WriteXML(string templatepath, string TempXMLPath, string outputxmlpath, DataTable datatablecsv, string SystemId_Templatepath, string filename, string filefullname, string filedirname, string ArchiveStorageFolderPath)
            {
                try
                {
                    int totalfilecnt = 0;
                    int processedfilecnt = 0;
                    int ignoredfilecnt = 0;
                    int filecnt = 0;

                    Log.Information($"[{ DateTime.Now}]  WriteXML Method started!");
                    Log.Information("templatepath:" + templatepath);
                    Log.Information("outputxmlpath:" + outputxmlpath);

                    XmlWriterSettings settings = new XmlWriterSettings
                    {
                        Encoding = Encoding.UTF8,
                        ConformanceLevel = ConformanceLevel.Document,
                        OmitXmlDeclaration = false,
                        CloseOutput = true,
                        Indent = true,
                        IndentChars = "  ",
                        NewLineHandling = NewLineHandling.None

                    };

                    XmlTextReader xmlreader = new XmlTextReader(templatepath);
                    //reading the xml data  
                    DataSet ds = new DataSet();
                    ds.ReadXml(xmlreader);
                    xmlreader.Close();

                    DataTable dt = new DataTable();
                    dt = ds.Tables[0];


                    XmlTextReader xmlreader1 = new XmlTextReader(SystemId_Templatepath);
                    //reading the xml data  
                    DataSet ds_systemid = new DataSet();
                    ds_systemid.ReadXml(xmlreader1);
                    xmlreader1.Close();

                    DataTable dt_systemid = new DataTable();
                    dt_systemid = ds_systemid.Tables[0];

                    Dictionary<string, string> dic_systemid_translation = new Dictionary<string, string>();
                    for (int i = 0; i < dt_systemid.Columns.Count; i++)
                    {
                        string key = dt_systemid.Columns[i].ColumnName;
                        string value = dt_systemid.Rows[0][key.ToString()].ToString();
                        dic_systemid_translation.Add(key, value);
                    }

                    DataTable dt_workt_stat = new DataTable();
                    dt_workt_stat = ds_systemid.Tables[2];

                    string finaloutputxmlpath;
                    string TempOutputXMLPath;
                    for (int i = 0; i < datatablecsv.Rows.Count; i++)
                    {
                        string systemid_value = datatablecsv.Rows[i]["SystemID"].ToString().Trim();
                        string policynumber = datatablecsv.Rows[i]["PolicyNumber"].ToString().Trim();
                        try
                        {
                           
                            bool xmlwritten = false;

                            if (dic_systemid_translation.ContainsKey(systemid_value) && policynumber != "NA")
                            {
                                if (dic_systemid_translation[systemid_value] != "Exclude")
                                {
                                    filecnt += 1;

                                    TempOutputXMLPath = TempXMLPath + "\\AWDRIP_" + System.IO.Path.GetFileName(filedirname) + "_" + filename + "_" + DateTime.Now.ToString("yyyy-MM-dd-hh-mm-ss") + "_" + filecnt.ToString() + ".xml";
                                    finaloutputxmlpath = outputxmlpath + "\\AWDRIP_" + System.IO.Path.GetFileName(filedirname) + "_" + filename + "_" + DateTime.Now.ToString("yyyy-MM-dd-hh-mm-ss") + "_" + filecnt.ToString() + ".xml";
                                    XmlWriter output = XmlWriter.Create(TempOutputXMLPath, settings);
                                    output.WriteStartDocument();
                                    output.WriteStartElement("AWDRIP");

                                    output.WriteAttributeString("xmlns", "xsd", null, "http://www.w3.org/2001/XMLSchema");
                                    output.WriteAttributeString("xmlns", "xsi", null, "http://www.w3.org/2001/XMLSchema-instance");
                                    output.WriteStartElement("transaction");
                                    for (int j = 0; j < dt.Columns.Count; j++)
                                    {


                                        string colname = dt.Columns[j].ColumnName.ToString();
                                        string colvalue = dt.Rows[0][j].ToString();
                                        string value_written = "";
                                        if (colname == "UNIT")
                                        {

                                            if (dic_systemid_translation.ContainsKey(systemid_value))
                                            {
                                                value_written = dic_systemid_translation[systemid_value];
                                            }
                                            else
                                            {
                                                value_written = datatablecsv.Rows[i][colvalue].ToString();
                                            }
                                        }
                                        else if (colname == "SOON")
                                        {
                                            value_written = colvalue;
                                        }
                                        else if (colname == "WRKT")
                                        {

                                            DataRow[] result = dt_workt_stat.Select(String.Format("SYSTEMID = '{0}'", systemid_value));

                                            value_written = result[0]["WRKT"].ToString();


                                        }
                                        else if (colname == "STAT")
                                        {
                                            DataRow[] result = dt_workt_stat.Select(String.Format("SYSTEMID = '{0}'", systemid_value));
                                            value_written = result[0]["STAT"].ToString();

                                        }

                                        else
                                        {
                                            value_written = datatablecsv.Rows[i][colvalue].ToString();
                                        }



                                        if (value_written.Trim() != "")
                                        {
                                            output.WriteStartElement("field");
                                            output.WriteAttributeString("value", value_written);
                                            output.WriteAttributeString("name", colname);
                                            output.WriteEndElement();
                                        }


                                    }
                                    output.WriteFullEndElement();
                                    output.WriteEndElement();
                                    output.WriteEndDocument();
                                    output.Close();

									CopyXMLFromTempToDest(TempOutputXMLPath, finaloutputxmlpath);

                                    processedfilecnt += 1;
                                    xmlwritten = true;

                                    Log.Information("XML Written,filename:" + filename + ",policyno:" + policynumber + ",rowno :" + (i + 1).ToString());

                                }
                            }

                            totalfilecnt += 1;
                            if (xmlwritten == false)
                            {
                                 Log.Information("row ignored,filename:" + filename + ",policyno:" + policynumber + ",ignored_rowno:" + (i + 1).ToString());
                            }

                        }
                        catch (System.Exception ex)
                        {
                            Log.Error("Error in Writing XML  " + ex.Message + "filename:" + filename + ", policyno: " + policynumber + ", rowno: " + (i + 1).ToString());
                        }
                        finally
                        {
                           
                        }
                    }

                    ignoredfilecnt = totalfilecnt - processedfilecnt;
                    Log.Information("final_totalrowcnt :" + totalfilecnt.ToString() + " ,final_processedrowcnt :" + processedfilecnt.ToString() + ",final_ignoredrowcnt :" + ignoredfilecnt.ToString() + ",filename:" + filename);




                    string destinationfilepath = ArchiveStorageFolderPath + "\\" + filename.Remove(filename.Length - 4) + "." + System.IO.Path.GetFileName(filedirname) + "_" + DateTime.Now.ToString("yyyy-MM-dd-hh-mm-ss") + ".csv";
                    MoveCSVFileToArchive(filefullname, destinationfilepath, ArchiveStorageFolderPath);


                    Log.Information($"[{ DateTime.Now}]  WriteXML Method ended!");
                }
                catch (System.Exception ex)
                {
                    Log.Error("Error in WriteXML method " + ex.Message);
                }
                finally
                {
                    
                }
            }



			/// <summary>
			/// Moving CSV files to Archivel folder
			/// </summary>
			/// <param name="sourcefilepath"></param>
			/// <param name="destinationfilepath"></param>
			/// <param name="filedirname"></param>
            public void MoveCSVFileToArchive(string sourcefilepath, string destinationfilepath, string filedirname)
            {
                try
                {
                    CreateIfMissing(filedirname);
                    File.Move(sourcefilepath, destinationfilepath);
                    Log.Information("ArchiveStoragePath:" + destinationfilepath);
                }
                catch (System.Exception ex)
                {
                    Log.Error("Error in MoveCSVFileToArchive method " + ex.Message);
                }
            }
			/// <summary>
			/// Copying XML files to destination folder
			/// </summary>
			/// <param name="sourcefilepath"></param>
			/// <param name="destinationfilepath"></param>
            public void CopyXMLFromTempToDest(string sourcefilepath, string destinationfilepath)
            {
                try
                {

                    File.Copy(sourcefilepath, destinationfilepath);
                    Log.Information("MoveXMLFromTempToDest:" + destinationfilepath);
                }
                catch (System.Exception ex)
                {
                    Log.Error("Error in MoveCSVFileToArchive method " + ex.Message);
                }
            }




        }

    }



}
