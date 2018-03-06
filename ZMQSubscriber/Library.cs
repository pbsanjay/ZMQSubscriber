using System;
using MySql.Data.MySqlClient;
using System.Text;
using ZMQSubscriber.Models;
using System.Net.Http;
using Newtonsoft.Json;
using System.Configuration;

namespace ZMQSubscriber
{
    class Library
    {
        private const int XLeft = 3;
        private const int XRight = 25;
        private const int YLeft = 10;
        private const int YRight = 3;
        private  int _minCheckConsecutiveShownDiffInSeconds = 5;
        private  int _maxCheckConsecutiveShownDiffInSeconds = 15;
        private  int _constSkipNotificationForSeconds = 30;
        private MySqlConnection _con = null;

        /// <summary>
        /// Default Constructor 
        /// </summary>
        public Library()
        {
            _con = new MySqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ToString());
            Init();
        }

        void Init()
        {
            try {
                _minCheckConsecutiveShownDiffInSeconds = (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["MINCheckConsecutiveShownDiffInSeconds"].ToString()) ? int.Parse(ConfigurationManager.AppSettings["MINCheckConsecutiveShownDiffInSeconds"].ToString()) : _minCheckConsecutiveShownDiffInSeconds);
            } catch (Exception ignore) {   }
            try
            {
                _maxCheckConsecutiveShownDiffInSeconds = (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["MAXCheckConsecutiveShownDiffInSeconds"].ToString()) ? int.Parse(ConfigurationManager.AppSettings["MAXCheckConsecutiveShownDiffInSeconds"].ToString()) : _maxCheckConsecutiveShownDiffInSeconds);
            }
            catch (Exception ignore) {   }
            try
            {
                _constSkipNotificationForSeconds = (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["constSkipNotificationForSeconds"].ToString()) ? int.Parse(ConfigurationManager.AppSettings["constSkipNotificationForSeconds"].ToString()) : _constSkipNotificationForSeconds);
            }
            catch (Exception ignore) {   }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="objLocationData"></param>
        /// <returns></returns>
  
        public bool InsertData(LocationData objLocationData)
        {
            try
            {
                MySqlCommand insertCommand = new MySqlCommand("INSERT INTO TrackMacNotification(MacAddress,LastVisitDateTime,LastNotifiedDateTime,SiteName) VALUES (@mac,@LastVistDateTime,@LastNotifiedDateTime,@SiteName)", _con);
                insertCommand.Parameters.Add(new MySqlParameter("@mac", objLocationData.mac));
                insertCommand.Parameters.Add(new MySqlParameter("@LastVistDateTime", objLocationData.LastSeenDatetime));
                // insertCommand.Parameters.Add(new MySqlParameter("@NotifiedDateTime", objLocationData.LastSeenDatetime));
                insertCommand.Parameters.Add(new MySqlParameter("@LastNotifiedDateTime", null));
                insertCommand.Parameters.Add(new MySqlParameter("@SiteName", objLocationData.sn));
                _con.Open();
                Console.WriteLine("Commands executed! Total rows affected are " + insertCommand.ExecuteNonQuery());
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _con.Close();
            }
            return true;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="objLocationData"></param>
        /// <returns></returns>
        public bool UpdateLastVisiteDate(LocationData objLocationData)
        {
            try
            {
              MySqlCommand updateCommand = new MySqlCommand("UPDATE TrackMacNotification SET LastVisitDateTime = '" + objLocationData.LastSeenDatetime.ToString("yyyy-MM-dd hh:MM:ss") + "' where mac='" + objLocationData.mac + "';", _con);

                _con.Open();
                Console.WriteLine("Commands executed! Total rows affected are " + updateCommand.ExecuteNonQuery());
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _con.Close();
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool UpdateNotificationData(LocationData objLocationData)
        {
            try
            {
          
                MySqlCommand updateCommand = new MySqlCommand("UPDATE TrackMacNotification SET LastVisitDateTime = '" + objLocationData.LastSeenDatetime.ToString("yyyy-MM-dd H:mm:ss") + "', LastNotifiedDateTime ='" + objLocationData.LastSeenDatetime.ToString("yyyy-MM-dd H:mm:ss") + "' where MacAddress='" + objLocationData.mac + "';", _con);
                _con.Open();
                Console.WriteLine("Commands executed! Total rows affected are " + updateCommand.ExecuteNonQuery());
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _con.Close();
            }
            return true;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
    
        public int IsMacAddressExist(LocationData objLocationData, int siteId)
        {
            int count = 0;
            try
            {
                MySqlCommand comm = new MySqlCommand("select count(*) from device a, deviceassociatesite b where b.DeviceId = a.DeviceId and  b.IsTrackByAdmin = 1 and b.SiteId = "+ siteId + "  and a.MacAddress='" + objLocationData.mac + "'", _con);
                _con.Open();
                count = int.Parse(comm.ExecuteScalar().ToString());
               
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                _con.Close();
            }
            return count;
        }
       
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
      public int IsNotificationSentBefore(LocationData objLocationData)
        {
            int countNotification = 0;
            try
            {
                MySqlCommand selectCommand = new MySqlCommand("SELECT COUNT(*) FROM TrackMacNotification Where MacAddress='" + objLocationData.mac + "'", _con);
                _con.Open();
                countNotification = int.Parse(selectCommand.ExecuteScalar().ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _con.Close();
            }
            return countNotification;
        }
    
        public DateTime LastVisitDateTimeForMacAddress(string mac)
        {
            DateTime notifiedDateTime;
            try
            {
                MySqlCommand selectCommand = new MySqlCommand("SELECT LastVisitDateTime FROM TrackMacNotification Where MacAddress='" + mac + "'", _con);
                _con.Open();
                notifiedDateTime = DateTime.Parse(selectCommand.ExecuteScalar().ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _con.Close();
            }
            return notifiedDateTime;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mac"></param>
        /// <returns></returns>
      
        public DateTime LastNotifiedDateTimeForMacAddress(string mac)
        {
            DateTime notifiedDateTime = DateTime.MinValue;
            try
            {
                MySqlCommand selectCommand = new MySqlCommand("SELECT LastNotifiedDateTime FROM TrackMacNotification Where MacAddress='" + mac + "'", _con);
                _con.Open();
                string val = selectCommand.ExecuteScalar().ToString();
                notifiedDateTime = (!string.IsNullOrEmpty(val) ? DateTime.Parse(val) : DateTime.MinValue);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _con.Close();
            }
            return notifiedDateTime;
        }

      
        public bool IsAlreadyNotified(string mac)
        {
            bool retVal = false;
            DateTime lastNotified = DateTime.MinValue;
            try
            {
              
                lastNotified = LastNotifiedDateTimeForMacAddress(mac);

            }
            catch (Exception ex)
            {
                throw ex;
            }
            if (lastNotified != null && lastNotified != DateTime.MinValue)       retVal = true;
            return retVal;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool IsSeenAfterConstantMinute(LocationData objLocationData)
        {

            try
            {
                DateTime lastVistDateTime = LastVisitDateTimeForMacAddress   (objLocationData.mac);
                Console.WriteLine("Last Vist Date Time - " + lastVistDateTime);
                Console.WriteLine("Seconds Diff" + objLocationData.LastSeenDatetime.Subtract(lastVistDateTime).Seconds);
                if (objLocationData.LastSeenDatetime.Subtract(lastVistDateTime).Seconds >= _constSkipNotificationForSeconds)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _con.Close();
            }
        }

        /// <summary>
        /// If difference between two datetime is 5s for Particular MacAddress then its a consecutice Data
        /// </summary>
        /// <returns></returns>
        public bool IsConsecutiveStreamDataForMac(LocationData objLocationData)
        {
            DateTime seenDateTime = LastVisitDateTimeForMacAddress(objLocationData.mac);
            Console.WriteLine("LastVist time - " + seenDateTime.ToString() +  "  For MacAddress -" + objLocationData.mac);
            int secDiff = objLocationData.LastSeenDatetime.Subtract(seenDateTime).Seconds;
            Console.WriteLine("Sec Diff - " + secDiff);
            if (secDiff >= _minCheckConsecutiveShownDiffInSeconds && secDiff <= _maxCheckConsecutiveShownDiffInSeconds)
            {
                return true;
            }
            else
            {
                return false;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="objLocationData"></param>
        /// <returns></returns>
        public bool PostRestCall(LocationData objLocationData)
        {
            Console.WriteLine("Enter into the PostRestCall");
            bool retBoolValue = false;
            objLocationData.PostDateTime = DateTime.Now;
            String resContent = JsonConvert.SerializeObject(objLocationData);
            try
            {
                //PostingTime
                using (HttpClient httpClient = new HttpClient())
                {
                    Console.WriteLine(ConfigurationManager.AppSettings["MermberShipApplication"].ToString());
                    httpClient.BaseAddress = new Uri(ConfigurationManager.AppSettings["MermberShipApplication"]);
                    var result = httpClient.PostAsync(httpClient.BaseAddress, new StringContent(resContent, Encoding.UTF8, "application/json")).Result;
                    if (result.IsSuccessStatusCode)
                    {
                        Console.WriteLine("Successfully sent to the Member Application");
                        var resultContent = result.Content.ReadAsStringAsync();
                        Notification objNotifications =
                            JsonConvert.DeserializeObject<Notification>(resultContent.Result);
                        Console.WriteLine(objNotifications.result.errmsg);
                        retBoolValue = objNotifications.result.returncode == 0;
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            return retBoolValue;
        }
    }
}
