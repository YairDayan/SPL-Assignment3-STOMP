#pragma once

#include <iostream>
#include <map>
#include <string>
#include <vector>

/**
 * A single emergency event: read from an events JSON file, or received in a
 * MESSAGE frame.
 */
class Event {
private:
  // name of channel
  std::string channel_name;
  // city of the event
  std::string city;
  // name of the event
  std::string name;
  // time of the event in seconds (epoch)
  int date_time;
  // description of the event
  std::string description;
  // map of all the general information ("active", "forces_arrival_at_scene")
  std::map<std::string, std::string> general_information;
  // name of the user who reported the event
  std::string eventOwnerUser;

public:
  /**
   * Creates an event from its fields. Used by parseEventsFile.
   *
   * @param channel_name        channel the event is reported to
   * @param city                city of the event
   * @param name                name of the event
   * @param date_time           time of the event in seconds (epoch)
   * @param description         description of the event
   * @param general_information "active" and "forces_arrival_at_scene" values
   */
  Event(std::string channel_name, std::string city, std::string name,
        int date_time, std::string description,
        std::map<std::string, std::string> general_information);

  /**
   * Creates an event by parsing the body of a MESSAGE frame.
   * The channel is not part of the body; set it with setChannelName.
   *
   * @param frame_body the frame body, in the report format
   */
  Event(const std::string &frame_body);

  virtual ~Event();

  /**
   * Sets the channel the event is reported to.
   *
   * @param channel_name channel the event is reported to
   */
  void setChannelName(const std::string &channel_name);

  /**
   * Sets the user who reported the event.
   *
   * @param setEventOwnerUser name of the user who reported the event
   */
  void setEventOwnerUser(std::string setEventOwnerUser);

  /**
   * Returns the user who reported the event.
   *
   * @return name of the user who reported the event
   */
  const std::string &getEventOwnerUser() const;

  /**
   * Returns the channel the event is reported to.
   *
   * @return channel the event is reported to
   */
  const std::string &get_channel_name() const;

  /**
   * Returns the city of the event.
   *
   * @return city of the event
   */
  const std::string &get_city() const;

  /**
   * Returns the description of the event.
   *
   * @return description of the event
   */
  const std::string &get_description() const;

  /**
   * Returns the name of the event.
   *
   * @return name of the event
   */
  const std::string &get_name() const;

  /**
   * Returns the time of the event.
   *
   * @return time of the event in seconds (epoch)
   */
  int get_date_time() const;

  /**
   * Returns the general information of the event.
   *
   * @return map of "active" and "forces_arrival_at_scene" to "true"/"false"
   */
  const std::map<std::string, std::string> &get_general_information() const;
};

/**
 * The content of an events file: the channel name and its events.
 * Returned by parseEventsFile.
 */
struct names_and_events {
  std::string channel_name;
  std::vector<Event> events;
};

/**
 * Parses an events JSON file.
 * date_time in the file may be an epoch number, an epoch string, or a
 * "DD/MM/YY(YY) HH:MM" / "DD/MM/YYYY_HH:MM" string; all are stored as epoch.
 *
 * @param json_path path to the events file
 * @return the channel name and the events in the file
 * @throws std::exception if the file cannot be parsed
 */
names_and_events parseEventsFile(std::string json_path);
