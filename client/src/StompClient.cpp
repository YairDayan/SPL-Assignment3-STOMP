#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <fstream>
#include <algorithm>
#include <ctime>
#include <chrono>
#include <cctype>

#include "../include/ConnectionHandler.h"
#include "../include/event.h"

struct ParsedFrame {
    std::string command;
    std::map<std::string, std::string> headers;
    std::string body;
};

struct EventRecord {
    std::string channel;
    std::string user;
    std::string city;
    std::string eventName;
    int dateTime;
    std::string description;
    std::map<std::string, std::string> generalInfo;

    EventRecord() : dateTime(0) {}
};

static std::string trim(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) start++;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) end--;
    return s.substr(start, end - start);
}

static bool startsWith(const std::string& s, const std::string& pref) {
    return s.size() >= pref.size() && s.compare(0, pref.size(), pref) == 0;
}

static std::string stripLeadingSlash(const std::string& s) {
    if (!s.empty() && s[0] == '/') return s.substr(1);
    return s;
}

static std::vector<std::string> splitWS(const std::string& line) {
    std::vector<std::string> out;
    std::istringstream iss(line);
    std::string tok;
    while (iss >> tok) out.push_back(tok);
    return out;
}

static ParsedFrame parseFrame(const std::string& frame) {
    ParsedFrame pf;
    std::string normalized = frame;
    normalized.erase(std::remove(normalized.begin(), normalized.end(), '\r'), normalized.end());

    size_t firstNl = normalized.find('\n');
    if (firstNl == std::string::npos) {
        pf.command = normalized;
        return pf;
    }

    pf.command = normalized.substr(0, firstNl);

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
        if (line.empty()) continue;
        size_t colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string k = trim(line.substr(0, colon));
        std::string v = trim(line.substr(colon + 1));
        pf.headers[k] = v;
    }

    return pf;
}

static EventRecord parseEventFromMessageBody(const std::string& body, const std::string& destination) {
    EventRecord rec;
    rec.channel = stripLeadingSlash(destination);

    std::istringstream ss(body);
    std::string line;
    bool inGeneral = false;
    bool inDesc = false;
    std::string desc;

    while (std::getline(ss, line)) {
        if (line.empty()) continue;

        if (line == "general information:") {
            inGeneral = true;
            inDesc = false;
            continue;
        }
        if (line == "description:") {
            inDesc = true;
            inGeneral = false;
            continue;
        }

        if (inDesc) {
            if (!desc.empty()) desc += "\n";
            desc += line;
            continue;
        }

        size_t colon = line.find(':');
        if (colon == std::string::npos) continue;

        std::string key = trim(line.substr(0, colon));
        std::string val = trim(line.substr(colon + 1));

        if (key == "user") rec.user = val;
        else if (key == "city") rec.city = val;
        else if (key == "event name") rec.eventName = val;
        else if (key == "date time") {
            try { rec.dateTime = std::stoi(val); } catch (...) { rec.dateTime = 0; }
        } else if (inGeneral) {
            rec.generalInfo[key] = val;
        }
    }

    rec.description = desc;
    return rec;
}

static std::string epochToDateTime(int epochSecs) {
    std::time_t t = static_cast<std::time_t>(epochSecs);
    std::tm* tmPtr = std::localtime(&t);
    if (tmPtr == nullptr) return "01/01/70 00:00";

    char buf[64];
    std::strftime(buf, sizeof(buf), "%d/%m/%y %H:%M", tmPtr);
    return std::string(buf);
}

static std::string summarizeDescription(const std::string& d) {
    if (d.size() <= 27) return d;
    return d.substr(0, 27) + "...";
}

int main(int argc, char* argv[]) {
    (void)argc;
    (void)argv;

    std::unique_ptr<ConnectionHandler> connection;
    std::thread readerThread;

    std::atomic<bool> loggedIn(false);
    std::atomic<bool> stopReader(false);
    std::atomic<bool> awaitingLogoutReceipt(false);

    std::mutex stateMutex;
    std::condition_variable logoutCv;

    std::string currentUser;
    int nextSubscriptionId = 1;
    int nextReceiptId = 1;

    std::unordered_map<std::string, int> channelToSubId;
    std::unordered_map<int, std::string> receiptAction;
    std::map<std::string, std::map<std::string, std::vector<EventRecord> > > eventsByChannelAndUser;

    auto closeConnection = [&]() {
        if (connection) connection->close();
    };

    auto sendFrame = [&](const std::string& frame) -> bool {
        if (!connection) return false;
        return connection->sendFrameAscii(frame, '\0');
    };

    auto storeEvent = [&](const EventRecord& rec) {
        std::lock_guard<std::mutex> lock(stateMutex);
        eventsByChannelAndUser[rec.channel][rec.user].push_back(rec);
    };

    auto stopAndJoinReader = [&]() {
        stopReader.store(true);
        closeConnection();
        if (readerThread.joinable()) readerThread.join();
    };

    auto handleServerFrame = [&](const std::string& rawFrame) {
        ParsedFrame pf = parseFrame(rawFrame);

        if (pf.command == "CONNECTED") {
            std::cout << "Login successful" << std::endl;
            return;
        }

        if (pf.command == "RECEIPT") {
            int rid = -1;
            std::map<std::string, std::string>::const_iterator it = pf.headers.find("receipt-id");
            if (it != pf.headers.end()) {
                try { rid = std::stoi(it->second); } catch (...) { rid = -1; }
            }

            std::string action;
            {
                std::lock_guard<std::mutex> lock(stateMutex);
                std::unordered_map<int, std::string>::iterator ait = receiptAction.find(rid);
                if (ait != receiptAction.end()) {
                    action = ait->second;
                    receiptAction.erase(ait);
                }
            }

            if (startsWith(action, "join:")) {
                std::cout << "Joined channel " << action.substr(5) << std::endl;
            } else if (startsWith(action, "exit:")) {
                std::cout << "Exited channel " << action.substr(5) << std::endl;
            } else if (action == "logout") {
                awaitingLogoutReceipt.store(false);
                loggedIn.store(false);
                stopReader.store(true);
                closeConnection();
                logoutCv.notify_all();
            }
            return;
        }

        if (pf.command == "MESSAGE") {
            std::string destination;
            std::map<std::string, std::string>::const_iterator dit = pf.headers.find("destination");
            if (dit != pf.headers.end()) destination = dit->second;

            EventRecord rec = parseEventFromMessageBody(pf.body, destination);
            if (!rec.channel.empty() && !rec.user.empty()) {
                storeEvent(rec);
            }
            return;
        }

        if (pf.command == "ERROR") {
            std::string msg;
            std::map<std::string, std::string>::const_iterator mit = pf.headers.find("message");
            if (mit != pf.headers.end()) msg = mit->second;

            if (!msg.empty()) std::cout << msg << std::endl;
            else std::cout << rawFrame << std::endl;

            awaitingLogoutReceipt.store(false);
            loggedIn.store(false);
            stopReader.store(true);
            closeConnection();
            logoutCv.notify_all();
            return;
        }
    };

    auto startReader = [&]() {
        stopReader.store(false);

        readerThread = std::thread([&]() {
            while (!stopReader.load()) {
                if (!connection) break;
                std::string frame;
                if (!connection->getFrameAscii(frame, '\0')) break;
                handleServerFrame(frame);
            }

            if (loggedIn.load()) {
                loggedIn.store(false);
                awaitingLogoutReceipt.store(false);
                logoutCv.notify_all();
            }
        });
    };

    std::string line;
    while (std::getline(std::cin, line)) {
        if (trim(line).empty()) continue;

        std::vector<std::string> args = splitWS(line);
        if (args.empty()) continue;

        std::string cmd = args[0];

        if (cmd == "login") {
            if (args.size() != 4) {
                std::cout << "Usage: login {host:port} {username} {password}" << std::endl;
                continue;
            }
            if (loggedIn.load()) {
                std::cout << "The client is already logged in, log out before trying again" << std::endl;
                continue;
            }

            std::string hostPort = args[1];
            size_t colon = hostPort.find(':');
            if (colon == std::string::npos) {
                std::cout << "Usage: login {host:port} {username} {password}" << std::endl;
                continue;
            }

            std::string host = hostPort.substr(0, colon);
            short port = 0;
            try { port = static_cast<short>(std::stoi(hostPort.substr(colon + 1))); }
            catch (...) {
                std::cout << "Usage: login {host:port} {username} {password}" << std::endl;
                continue;
            }

            std::unique_ptr<ConnectionHandler> newConn(new ConnectionHandler(host, port));
            if (!newConn->connect()) {
                std::cout << "Could not connect to server" << std::endl;
                continue;
            }

            connection = std::move(newConn);
            currentUser = args[2];
            loggedIn.store(true);

            {
                std::lock_guard<std::mutex> lock(stateMutex);
                channelToSubId.clear();
                receiptAction.clear();
            }

            startReader();

            std::string frame =
                "CONNECT\n"
                "accept-version:1.2\n"
                "host:stomp.cs.bgu.ac.il\n"
                "login:" + args[2] + "\n"
                "passcode:" + args[3] + "\n\n";

            if (!sendFrame(frame)) {
                std::cout << "Could not connect to server" << std::endl;
                loggedIn.store(false);
                stopAndJoinReader();
            }
            continue;
        }

        if (!loggedIn.load()) {
            std::cout << "Please login first" << std::endl;
            continue;
        }

        if (cmd == "join") {
            if (args.size() != 2) {
                std::cout << "Usage: join {channel_name}" << std::endl;
                continue;
            }

            std::string channel = args[1];
            int sid = nextSubscriptionId++;
            int rid = nextReceiptId++;

            {
                std::lock_guard<std::mutex> lock(stateMutex);
                channelToSubId[channel] = sid;
                receiptAction[rid] = "join:" + channel;
            }

            std::string frame =
                "SUBSCRIBE\n"
                "destination:/" + channel + "\n"
                "id:" + std::to_string(sid) + "\n"
                "receipt:" + std::to_string(rid) + "\n\n";
            sendFrame(frame);
            continue;
        }

        if (cmd == "exit") {
            if (args.size() != 2) {
                std::cout << "Usage: exit {channel_name}" << std::endl;
                continue;
            }

            std::string channel = args[1];
            int sid = -1;
            {
                std::lock_guard<std::mutex> lock(stateMutex);
                std::unordered_map<std::string, int>::iterator it = channelToSubId.find(channel);
                if (it != channelToSubId.end()) sid = it->second;
            }

            if (sid == -1) {
                std::cout << "Not subscribed to channel " << channel << std::endl;
                continue;
            }

            int rid = nextReceiptId++;
            {
                std::lock_guard<std::mutex> lock(stateMutex);
                receiptAction[rid] = "exit:" + channel;
                channelToSubId.erase(channel);
            }

            std::string frame =
                "UNSUBSCRIBE\n"
                "id:" + std::to_string(sid) + "\n"
                "receipt:" + std::to_string(rid) + "\n\n";
            sendFrame(frame);
            continue;
        }

        if (cmd == "report") {
            if (args.size() != 2) {
                std::cout << "Usage: report {file}" << std::endl;
                continue;
            }

            names_and_events nae;
            try {
                nae = parseEventsFile(args[1]);
            } catch (...) {
                std::cout << "Failed to parse events file" << std::endl;
                continue;
            }

            std::string channel = trim(nae.channel_name);

            for (std::vector<Event>::const_iterator eit = nae.events.begin(); eit != nae.events.end(); ++eit) {
                const Event& ev = *eit;

                EventRecord rec;
                rec.channel = channel;
                rec.user = currentUser;
                rec.city = ev.get_city();
                rec.eventName = ev.get_name();
                rec.dateTime = ev.get_date_time();
                rec.description = ev.get_description();
                rec.generalInfo = ev.get_general_information();

                storeEvent(rec);

                std::string body;
                body += "user:" + currentUser + "\n";
                body += "city:" + rec.city + "\n";
                body += "event name:" + rec.eventName + "\n";
                body += "date time:" + std::to_string(rec.dateTime) + "\n";
                body += "general information:\n";
                for (std::map<std::string, std::string>::const_iterator git = rec.generalInfo.begin(); git != rec.generalInfo.end(); ++git) {
                    body += git->first + ":" + git->second + "\n";
                }
                body += "description:\n";
                body += rec.description;

                std::string frame =
                    "SEND\n"
                    "destination:/" + channel + "\n\n" +
                    body;

                sendFrame(frame);
            }
            continue;
        }

        if (cmd == "summary") {
            if (args.size() != 4) {
                std::cout << "Usage: summary {channel_name} {user} {file}" << std::endl;
                continue;
            }

            std::string channel = args[1];
            std::string user = args[2];
            std::string outFile = args[3];

            std::vector<EventRecord> events;
            {
                std::lock_guard<std::mutex> lock(stateMutex);
                std::map<std::string, std::map<std::string, std::vector<EventRecord> > >::iterator cit =
                    eventsByChannelAndUser.find(channel);
                if (cit != eventsByChannelAndUser.end()) {
                    std::map<std::string, std::vector<EventRecord> >::iterator uit = cit->second.find(user);
                    if (uit != cit->second.end()) events = uit->second;
                }
            }

            std::sort(events.begin(), events.end(),
                      [](const EventRecord& a, const EventRecord& b) {
                          if (a.dateTime != b.dateTime) return a.dateTime < b.dateTime;
                          return a.eventName < b.eventName;
                      });

            int activeCnt = 0;
            int forcesCnt = 0;
            for (std::vector<EventRecord>::const_iterator it = events.begin(); it != events.end(); ++it) {
                std::map<std::string, std::string>::const_iterator ait = it->generalInfo.find("active");
                if (ait != it->generalInfo.end() && ait->second == "true") activeCnt++;

                std::map<std::string, std::string>::const_iterator fit = it->generalInfo.find("forces_arrival_at_scene");
                if (fit != it->generalInfo.end() && fit->second == "true") forcesCnt++;
            }

            std::ofstream ofs(outFile.c_str(), std::ios::trunc);
            if (!ofs.is_open()) {
                std::cout << "Failed to write summary file" << std::endl;
                continue;
            }

            ofs << "Channel " << channel << "\n";
            ofs << "Stats:\n";
            ofs << "Total: " << events.size() << "\n";
            ofs << "active: " << activeCnt << "\n";
            ofs << "forces arrival at scene: " << forcesCnt << "\n";
            ofs << "Event Reports:\n";

            for (size_t i = 0; i < events.size(); i++) {
                ofs << "Report_" << (i + 1) << ":\n";
                ofs << "city: " << events[i].city << "\n";
                ofs << "date time: " << epochToDateTime(events[i].dateTime) << "\n";
                ofs << "event name: " << events[i].eventName << "\n";
                ofs << "summary: " << summarizeDescription(events[i].description) << "\n";
            }

            ofs.close();
            continue;
        }

        if (cmd == "logout") {
            int rid = nextReceiptId++;
            {
                std::lock_guard<std::mutex> lock(stateMutex);
                receiptAction[rid] = "logout";
            }

            awaitingLogoutReceipt.store(true);

            std::string frame =
                "DISCONNECT\n"
                "receipt:" + std::to_string(rid) + "\n\n";

            sendFrame(frame);

            std::mutex waitMutex;
            std::unique_lock<std::mutex> lk(waitMutex);
            bool gotReceipt = logoutCv.wait_for(
                lk,
                std::chrono::seconds(5),
                [&]() { return !awaitingLogoutReceipt.load(); }
            );

            if (!gotReceipt) {
                awaitingLogoutReceipt.store(false);
                loggedIn.store(false);
                stopAndJoinReader();
            } else {
                if (readerThread.joinable()) readerThread.join();
            }

            continue;
        }

        std::cout << "Unknown command" << std::endl;
    }

    loggedIn.store(false);
    stopAndJoinReader();
    return 0;
}