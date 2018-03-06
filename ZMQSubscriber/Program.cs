using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZeroMQ;
using ZMQSubscriber.Models;
using Newtonsoft.Json;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Configuration;

namespace ZMQSubscriber
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                System.IO.Directory.SetCurrentDirectory(System.AppDomain.CurrentDomain.BaseDirectory);
                string topicName = ConfigurationManager.AppSettings["TopicName"].ToString();
                string fatiNotificationServerUrl = ConfigurationManager.AppSettings["FatiNotificationServerUrl"].ToString();
                string areaNames = ConfigurationManager.AppSettings["AreaNames"].ToString();
                int  siteId = string.IsNullOrEmpty(ConfigurationManager.AppSettings["SiteId"].ToString())?0:int.Parse(ConfigurationManager.AppSettings["SiteId"].ToString());

                using (var context = new ZContext())
                using (var subscriber = new ZSocket(context, ZSocketType.SUB))
                {
                    //Create the Subscriber Connection
                    subscriber.Connect(fatiNotificationServerUrl);
                    subscriber.Subscribe(topicName);
                    Console.WriteLine("Subscriber started for Topic with URL : {0} {1}", topicName, fatiNotificationServerUrl);
                    Console.WriteLine("Site ID set to : {0}", siteId);
                    Library objLibray = new Library();
                    int subscribed = 0;

                    while (true)
                    {
                        using (ZMessage message = subscriber.ReceiveMessage())
                        {
                            subscribed++;

                            // Read message contents
                            string contents = message[1].ReadString();

                            Console.WriteLine(contents);

                            LocationData objLocationData = JsonConvert.DeserializeObject<ListOfArea>(contents).device_notification.records.FirstOrDefault();

                            
                            bool checkSeenAfterConstantMinute = false;
                            bool checkConsecutiveVisit = false;
                            bool checkAlreadyNotifiedOnce = false;

                            //if the MacAddress is Registered through our RTLS Service Layer
                            if (objLibray.IsMacAddressExist(objLocationData,siteId) > 0)
                            {

                                Console.WriteLine(objLocationData.mac + " - Mac Address Exist");

                                string[] watchArea = (string.IsNullOrEmpty(areaNames) ? null : areaNames.Split(','));
                                if (watchArea != null && watchArea.Length > 0)
                                {
                                    string[] inputArea = objLocationData.an;
                                    foreach (string an in inputArea)
                                    {
                                        if (watchArea.Contains(an.Trim()))
                                        {
                                            Console.WriteLine("Checking for Area [" + an + "]");

                                            DateTime macFoundDatetime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Local).AddSeconds(objLocationData.last_seen_ts);
                                            objLocationData.LastSeenDatetime = macFoundDatetime;

                                            //Alrearady Exist MacAddress to update the Notification LastSeenDateTime
                                            if (objLibray.IsNotificationSentBefore(objLocationData) >= 1)
                                            {
                                                
                                                checkSeenAfterConstantMinute = objLibray.IsSeenAfterConstantMinute(objLocationData);
                                                checkConsecutiveVisit = objLibray.IsConsecutiveStreamDataForMac(objLocationData);
                                                checkAlreadyNotifiedOnce = objLibray.IsAlreadyNotified(objLocationData.mac);

                                                Console.WriteLine(Environment.NewLine + "Check Seen After Constant Seconds - " + checkSeenAfterConstantMinute);
                                                Console.WriteLine("Check Consecutive Visit - " + checkConsecutiveVisit);
                                                Console.WriteLine("Check Already Notified Once - " + checkAlreadyNotifiedOnce);


                                                if (checkConsecutiveVisit == true && checkAlreadyNotifiedOnce == false)
                                                {
                                                    Console.WriteLine("Notifiy Visit.");
                                                    objLibray.PostRestCall(objLocationData);
                                                    objLibray.UpdateNotificationData(objLocationData);
                                                }
                                                else if (checkSeenAfterConstantMinute == true && checkAlreadyNotifiedOnce == true)
                                                {
                                                    Console.WriteLine("Notifiy Visit ");
                                                    objLibray.PostRestCall(objLocationData);
                                                    objLibray.UpdateNotificationData(objLocationData);
                                                }
                                            }
                                            //New MacAddress For Storing in Notification table.
                                            else
                                            {
                                                objLibray.InsertData(objLocationData);
                                                objLibray.PostRestCall(objLocationData);
                                                objLibray.UpdateNotificationData(objLocationData);
                                            }

                                        }
                                    }

                                }

                            }

                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Write(" Error:-"+ ex.Message);
                 
            }
        }
    }
}
