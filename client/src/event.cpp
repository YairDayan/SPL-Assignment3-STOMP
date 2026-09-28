#include "../include/event.h"
#include "../include/json.hpp"
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <cstring>
#include <algorithm>
#include <cctype>
#include <ctime>
#include <stdexcept>


using namespace std;
using json = nlohmann::json;

Event::Event(std::string channel_name, std::string city, std::string name, int date_time,
             std::string description, std::map<std::string, std::string> general_information)
    : channel_name(channel_name), city(city), name(name),
      date_time(date_time), description(description), general_information(general_information), eventOwnerUser("")
{
}

Event::~Event()
{
}

static void split_str(const std::string& input, char delim, std::vector<std::string>& out) {
    std::stringstream ss(input);
    std::string part;
    while (std::getline(ss, part, delim)) out.push_back(part);
}

void Event::setEventOwnerUser(std::string setEventOwnerUser) {
    eventOwnerUser = setEventOwnerUser;
}

const std::string &Event::getEventOwnerUser() const {
    return eventOwnerUser;
}

const std::string &Event::get_channel_name() const
{
    return this->channel_name;
}

const std::string &Event::get_city() const
{
    return this->city;
}

const std::string &Event::get_name() const
{
    return this->name;
}

int Event::get_date_time() const
{
    return this->date_time;
}

const std::map<std::string, std::string> &Event::get_general_information() const
{
    return this->general_information;
}

const std::string &Event::get_description() const
{
    return this->description;
}

Event::Event(const std::string &frame_body): channel_name(""), city(""), 
                                             name(""), date_time(0), description(""), general_information(),
                                             eventOwnerUser("")
{
    stringstream ss(frame_body);
    string line;
    string eventDescription;
    map<string, string> general_information_from_string;
    bool inGeneralInformation = false;
    while(getline(ss,line,'\n')){
        vector<string> lineArgs;
        if(line.find(':') != string::npos) {
            split_str(line, ':', lineArgs);
            string key = lineArgs.at(0);
            string val;
            if(lineArgs.size() == 2) {
                val = lineArgs.at(1);
            }
            if(key == "user") {
                eventOwnerUser = val;
            }
            if(key == "channel name") {
                channel_name = val;
            }
            if(key == "city") {
                city = val;
            }
            else if(key == "event name") {
                name = val;
            }
            else if(key == "date time") {
                date_time = std::stoi(val);
            }
            else if(key == "general information") {
                inGeneralInformation = true;
                continue;
            }
            else if(key == "description") {
                while(getline(ss,line,'\n')) {
                    eventDescription += line + "\n";
                }
                description = eventDescription;
            }

            if(inGeneralInformation) {
                general_information_from_string[key.substr(1)] = val;
            }
        }
    }
    general_information = general_information_from_string;
}

static std::string trim_copy(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) start++;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) end--;
    return s.substr(start, end - start);
}
static bool parseDateTimeFlexible(const std::string& raw, int& outEpoch) {
    std::string s = trim_copy(raw);
    if (s.empty()) return false;
    // Case 1: already epoch (e.g. "1762966800")
    bool allDigits = std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isdigit(c); });
    if (allDigits) {
        try {
            outEpoch = std::stoi(s);
            return true;
        } catch (...) {
            return false;
        }
    }
    // Case 2: assignment file format "DD/MM/YY HH:MM" or "DD/MM/YYYY HH:MM"
    int dd = 0, mm = 0, yy = 0, hh = 0, min = 0;
    if (std::sscanf(s.c_str(), "%d/%d/%d %d:%d", &dd, &mm, &yy, &hh, &min) != 5) {
        return false;
    }
    if (yy < 100) yy += 2000;
    std::tm tmVal{};
    tmVal.tm_mday = dd;
    tmVal.tm_mon = mm - 1;
    tmVal.tm_year = yy - 1900;
    tmVal.tm_hour = hh;
    tmVal.tm_min = min;
    tmVal.tm_sec = 0;
    tmVal.tm_isdst = -1; // let system determine DST
    std::time_t t = std::mktime(&tmVal);
    if (t == static_cast<std::time_t>(-1)) return false;
    outEpoch = static_cast<int>(t);
    return true;
}

names_and_events parseEventsFile(std::string json_path)
{
    std::ifstream f(json_path);
    json data = json::parse(f);

    std::string channel_name = data["channel_name"];

    // run over all the events and convert them to Event objects
    std::vector<Event> events;
    for (auto &event : data["events"])
    {
        std::string name = event["event_name"];
        std::string city = event["city"];
        int date_time = 0;
        if (event["date_time"].is_number_integer()) {
            date_time = event["date_time"].get<int>();
        } else if (event["date_time"].is_string()) {
            std::string dt = event["date_time"].get<std::string>();
            if (!parseDateTimeFlexible(dt, date_time)) {
                throw std::runtime_error("Invalid date_time format in events file: " + dt);
            }
        } else {
            throw std::runtime_error("Unsupported date_time type in events file");
        }
        std::string description = event["description"];
        std::map<std::string, std::string> general_information;
        for (auto &update : event["general_information"].items())
        {
            if (update.value().is_string())
                general_information[update.key()] = update.value();
            else
                general_information[update.key()] = update.value().dump();
        }

        events.push_back(Event(channel_name, city, name, date_time, description, general_information));
    }
    names_and_events events_and_names{channel_name, events};

    return events_and_names;
}
