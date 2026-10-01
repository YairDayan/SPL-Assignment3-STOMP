#include "../include/event.h"
#include "../include/json.hpp"
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <cstring>
#include <ctime>
#include <iomanip>
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

/**
 * Removes leading and trailing whitespace.
 *
 * @param s the string to trim
 * @return a trimmed copy of s
 */
static std::string trim_copy(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) start++;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) end--;
    return s.substr(start, end - start);
}

void Event::setChannelName(const std::string &channel_name) {
    this->channel_name = channel_name;
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
    // Set after the "general information:" line; the following key:value lines belong to general_information.
    bool inGeneralInformation = false;
    // Set after the "description:" line; all the following lines are the description.
    bool inDescription = false;
    while (getline(ss, line, '\n')) {
        if (line.empty()) continue;

        if (line == "general information:") {
            inGeneralInformation = true;
            continue;
        }
        if (line == "description:") {
            inDescription = true;
            inGeneralInformation = false;
            continue;
        }
        if (inDescription) {
            if (!description.empty()) description += "\n";
            description += line;
            continue;
        }

        // Split at the first ':' only, so values may contain ':'.
        size_t colon = line.find(':');
        if (colon == string::npos) continue;
        string key = trim_copy(line.substr(0, colon));
        string val = trim_copy(line.substr(colon + 1));

        if (key == "user") eventOwnerUser = val;
        else if (key == "channel name") channel_name = val;
        else if (key == "city") city = val;
        else if (key == "event name") name = val;
        else if (key == "date time") {
            try { date_time = std::stoi(val); } catch (...) { date_time = 0; }
        }
        else if (inGeneralInformation) general_information[key] = val;
    }
}

/**
 * Converts a date_time string from the events file to epoch seconds.
 * Accepted formats: epoch digits ("1762966800"), or a local date "DD/MM/YY HH:MM" / "DD/MM/YYYY HH:MM",
 * where the date and time may be separated by a space or '_'.
 *
 * @param s the date string
 * @return the time in epoch seconds
 * @throws std::runtime_error if s is in none of the accepted formats
 */
static int dateStringToEpoch(const std::string& s) {
    if (!s.empty() && std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isdigit(c); }))
        return std::stoi(s);

    int dd = 0, mm = 0, yy = 0, hh = 0, min = 0;
    if (std::sscanf(s.c_str(), "%d/%d/%d%*[ _]%d:%d", &dd, &mm, &yy, &hh, &min) != 5)
        throw std::runtime_error("Invalid date_time: " + s);
    if (yy < 100)
        yy += 2000;

    std::tm tm{};
    tm.tm_mday = dd;
    tm.tm_mon = mm - 1;
    tm.tm_year = yy - 1900;
    tm.tm_hour = hh;
    tm.tm_min = min;
    tm.tm_isdst = -1; // let mktime determine daylight saving time
    return static_cast<int>(std::mktime(&tm));
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
        // date_time may appear in the file as a JSON number or as a string.
        int date_time = event["date_time"].is_string()
                            ? dateStringToEpoch(event["date_time"].get<std::string>())
                            : event["date_time"].get<int>();
        std::string description = event["description"];
        std::map<std::string, std::string> general_information;
        // Non-string values (the booleans) are stored as their JSON text: "true" / "false".
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
