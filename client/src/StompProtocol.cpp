#include "../include/StompProtocol.h"

#include <algorithm>
#include <cctype>
#include <ctime>
#include <fstream>
#include <iostream>
#include <sstream>

/**
 * A STOMP frame split into its parts.
 */
struct ParsedFrame {
  std::string command;                        // e.g. "MESSAGE"
  std::map<std::string, std::string> headers; // header name -> value
  std::string body;                           // everything after the blank line

  ParsedFrame() : command(), headers(), body() {}
};

/**
 * Removes leading and trailing whitespace.
 *
 * @param s the string to trim
 * @return a trimmed copy of s
 */
static std::string trim(const std::string &s) {
  size_t start = 0;
  while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start])))
    start++;
  size_t end = s.size();
  while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1])))
    end--;
  return s.substr(start, end - start);
}

/**
 * Checks whether a string starts with a prefix.
 *
 * @param s    the string to check
 * @param pref the prefix
 * @return true if s starts with pref
 */
static bool startsWith(const std::string &s, const std::string &pref) {
  return s.size() >= pref.size() && s.compare(0, pref.size(), pref) == 0;
}

/**
 * Converts a destination header ("/police") to a channel name ("police").
 *
 * @param s the destination
 * @return s without its leading '/', if it has one
 */
static std::string stripLeadingSlash(const std::string &s) {
  if (!s.empty() && s[0] == '/')
    return s.substr(1);
  return s;
}

/**
 * Splits a raw frame into command, headers and body.
 *
 * @param frame the frame, without the terminating '\0'
 * @return the parsed frame; missing parts are left empty
 */
static ParsedFrame parseFrame(const std::string &frame) {
  ParsedFrame pf;
  std::string normalized = frame;
  // Accept "\r\n" line endings as well as "\n".
  normalized.erase(std::remove(normalized.begin(), normalized.end(), '\r'),
                   normalized.end());

  size_t firstNl = normalized.find('\n');
  if (firstNl == std::string::npos) {
    pf.command = normalized;
    return pf;
  }

  pf.command = normalized.substr(0, firstNl);

  // A blank line separates the headers from the body.
  size_t headersStart = firstNl + 1;
  size_t bodySep = normalized.find("\n\n", headersStart);

  std::string headersPart;
  if (bodySep == std::string::npos) {
    headersPart = normalized.substr(headersStart);
    pf.body = "";
  } else {
    headersPart = normalized.substr(headersStart, bodySep - headersStart);
    pf.body = normalized.substr(bodySep + 2);
  }

  std::istringstream hss(headersPart);
  std::string line;
  while (std::getline(hss, line)) {
    if (line.empty())
      continue;
    size_t colon = line.find(':');
    if (colon == std::string::npos)
      continue;
    std::string k = trim(line.substr(0, colon));
    std::string v = trim(line.substr(colon + 1));
    pf.headers[k] = v;
  }

  return pf;
}

/**
 * Formats an epoch time as a local "DD/MM/YY HH:MM" string, as required in the
 * summary.
 *
 * @param epochSecs time in seconds (epoch)
 * @return the formatted date and time
 */
static std::string epochToDateTime(int epochSecs) {
  std::time_t t = static_cast<std::time_t>(epochSecs);
  std::tm *tmPtr = std::localtime(&t);
  if (tmPtr == nullptr)
    return "01/01/70 00:00";

  char buf[64];
  std::strftime(buf, sizeof(buf), "%d/%m/%y %H:%M", tmPtr);
  return std::string(buf);
}

/**
 * Shortens a description for the summary: the first 27 characters, followed by
 * "..." if it was longer.
 *
 * @param d the description
 * @return the shortened description
 */
static std::string summarizeDescription(const std::string &d) {
  if (d.size() <= 27)
    return d;
  return d.substr(0, 27) + "...";
}

StompProtocol::StompProtocol()
    : mutex_(), currentUser_(), nextSubscriptionId_(1), nextReceiptId_(1),
      channelToSubId_(), receiptAction_(), eventsByChannelAndUser_() {}

void StompProtocol::storeEvent(const Event &event) {
  std::lock_guard<std::mutex> lock(mutex_);
  eventsByChannelAndUser_[event.get_channel_name()][event.getEventOwnerUser()]
      .push_back(event);
}

void StompProtocol::startSession(const std::string &user) {
  std::lock_guard<std::mutex> lock(mutex_);
  currentUser_ = user;
  channelToSubId_.clear();
  receiptAction_.clear();
}

std::string StompProtocol::buildConnectFrame(const std::string &user,
                                             const std::string &passcode) {
  return "CONNECT\n"
         "accept-version:1.2\n"
         "host:stomp.cs.bgu.ac.il\n"
         "login:" +
         user +
         "\n"
         "passcode:" +
         passcode + "\n\n";
}

bool StompProtocol::buildSubscribeFrame(const std::string &channel,
                                        std::string &frame) {
  int sid;
  int rid;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // The server rejects a second subscription of the same connection to a
    // channel with an ERROR.
    if (channelToSubId_.count(channel) > 0)
      return false;
    sid = nextSubscriptionId_++;
    rid = nextReceiptId_++;
    channelToSubId_[channel] = sid;
    // Remembered so that handleServerFrame knows what the RECEIPT with this id
    // confirms.
    receiptAction_[rid] = "join:" + channel;
  }

  frame = "SUBSCRIBE\n"
          "destination:/" +
          channel +
          "\n"
          "id:" +
          std::to_string(sid) +
          "\n"
          "receipt:" +
          std::to_string(rid) + "\n\n";
  return true;
}

bool StompProtocol::buildUnsubscribeFrame(const std::string &channel,
                                          std::string &frame) {
  int sid;
  int rid;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, int>::iterator it =
        channelToSubId_.find(channel);
    if (it == channelToSubId_.end())
      return false;
    sid = it->second;
    rid = nextReceiptId_++;
    receiptAction_[rid] = "exit:" + channel;
    channelToSubId_.erase(it);
  }

  frame = "UNSUBSCRIBE\n"
          "id:" +
          std::to_string(sid) +
          "\n"
          "receipt:" +
          std::to_string(rid) + "\n\n";
  return true;
}

std::vector<std::string>
StompProtocol::buildReportFrames(const names_and_events &nae) {
  std::string user;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    user = currentUser_;
  }

  std::string channel = trim(nae.channel_name);
  std::vector<std::string> frames;

  for (std::vector<Event>::const_iterator eit = nae.events.begin();
       eit != nae.events.end(); ++eit) {
    Event ev = *eit;
    ev.setChannelName(channel);
    ev.setEventOwnerUser(user);

    // Own reports are stored here; the copy the server sends back as MESSAGE is
    // ignored.
    storeEvent(ev);

    std::string body;
    body += "user:" + user + "\n";
    body += "city:" + ev.get_city() + "\n";
    body += "event name:" + ev.get_name() + "\n";
    body += "date time:" + std::to_string(ev.get_date_time()) + "\n";
    body += "general information:\n";
    const std::map<std::string, std::string> &info =
        ev.get_general_information();
    for (std::map<std::string, std::string>::const_iterator git = info.begin();
         git != info.end(); ++git) {
      body += "\t" + git->first + ":" + git->second + "\n";
    }
    body += "description:\n";
    body += ev.get_description();

    frames.push_back("SEND\n"
                     "destination:/" +
                     channel + "\n\n" + body);
  }

  return frames;
}

std::string StompProtocol::buildDisconnectFrame() {
  int rid;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    rid = nextReceiptId_++;
    receiptAction_[rid] = "logout";
  }

  return "DISCONNECT\n"
         "receipt:" +
         std::to_string(rid) + "\n\n";
}

bool StompProtocol::writeSummary(const std::string &channel,
                                 const std::string &user,
                                 const std::string &file) {
  // Copy the reports under the lock, then sort and write the file without
  // holding it.
  std::vector<Event> events;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    std::map<std::string, std::map<std::string, std::vector<Event>>>::iterator
        cit = eventsByChannelAndUser_.find(channel);
    if (cit != eventsByChannelAndUser_.end()) {
      std::map<std::string, std::vector<Event>>::iterator uit =
          cit->second.find(user);
      if (uit != cit->second.end())
        events = uit->second;
    }
  }

  std::sort(events.begin(), events.end(), [](const Event &a, const Event &b) {
    if (a.get_date_time() != b.get_date_time())
      return a.get_date_time() < b.get_date_time();
    return a.get_name() < b.get_name();
  });

  int activeCnt = 0;
  int forcesCnt = 0;
  for (std::vector<Event>::const_iterator it = events.begin();
       it != events.end(); ++it) {
    const std::map<std::string, std::string> &info =
        it->get_general_information();

    std::map<std::string, std::string>::const_iterator ait =
        info.find("active");
    if (ait != info.end() && ait->second == "true")
      activeCnt++;

    std::map<std::string, std::string>::const_iterator fit =
        info.find("forces_arrival_at_scene");
    if (fit != info.end() && fit->second == "true")
      forcesCnt++;
  }

  std::ofstream ofs(file.c_str(), std::ios::trunc);
  if (!ofs.is_open())
    return false;

  ofs << "Channel " << channel << "\n";
  ofs << "Stats:\n";
  ofs << "Total: " << events.size() << "\n";
  ofs << "active: " << activeCnt << "\n";
  ofs << "forces arrival at scene: " << forcesCnt << "\n";
  ofs << "\nEvent Reports:\n";

  for (size_t i = 0; i < events.size(); i++) {
    ofs << "\nReport_" << (i + 1) << ":\n";
    ofs << "\tcity: " << events[i].get_city() << "\n";
    ofs << "\tdate time: " << epochToDateTime(events[i].get_date_time())
        << "\n";
    ofs << "\tevent name: " << events[i].get_name() << "\n";
    ofs << "\tsummary: " << summarizeDescription(events[i].get_description())
        << "\n";
  }

  ofs.close();
  return true;
}

FrameResult StompProtocol::handleServerFrame(const std::string &rawFrame) {
  ParsedFrame pf = parseFrame(rawFrame);

  if (pf.command == "CONNECTED") {
    std::cout << "Login successful" << std::endl;
    return FrameResult::LOGIN_SUCCESS;
  }

  if (pf.command == "RECEIPT") {
    int rid = -1;
    std::map<std::string, std::string>::const_iterator it =
        pf.headers.find("receipt-id");
    if (it != pf.headers.end()) {
      try {
        rid = std::stoi(it->second);
      } catch (...) {
        rid = -1;
      }
    }

    std::string action;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      std::unordered_map<int, std::string>::iterator ait =
          receiptAction_.find(rid);
      if (ait != receiptAction_.end()) {
        action = ait->second;
        receiptAction_.erase(ait);
      }
    }

    if (startsWith(action, "join:")) {
      std::cout << "Joined channel " << action.substr(5) << std::endl;
    } else if (startsWith(action, "exit:")) {
      std::cout << "Exited channel " << action.substr(5) << std::endl;
    } else if (action == "logout") {
      return FrameResult::LOGOUT_RECEIPT;
    }
    return FrameResult::NONE;
  }

  if (pf.command == "MESSAGE") {
    std::string destination;
    std::map<std::string, std::string>::const_iterator dit =
        pf.headers.find("destination");
    if (dit != pf.headers.end())
      destination = dit->second;

    std::string user;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      user = currentUser_;
    }

    // The channel is not in the body, only in the destination header.
    Event ev(pf.body);
    ev.setChannelName(stripLeadingSlash(destination));
    // Own reports were already stored when sent.
    if (!ev.get_channel_name().empty() && !ev.getEventOwnerUser().empty() &&
        ev.getEventOwnerUser() != user) {
      storeEvent(ev);
    }
    return FrameResult::NONE;
  }

  if (pf.command == "ERROR") {
    std::string msg;
    std::map<std::string, std::string>::const_iterator mit =
        pf.headers.find("message");
    if (mit != pf.headers.end())
      msg = mit->second;

    // Print the message header if present, otherwise the entire frame.
    if (!msg.empty())
      std::cout << msg << std::endl;
    else
      std::cout << rawFrame << std::endl;
    return FrameResult::ERROR_FRAME;
  }

  return FrameResult::NONE;
}
