#pragma once

#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "../include/event.h"

/**
 * Tells the client what to do with the connection after a server frame was
 * handled.
 */
enum class FrameResult {
  NONE,           // nothing to do
  LOGIN_SUCCESS,  // CONNECTED received
  LOGOUT_RECEIPT, // RECEIPT for the DISCONNECT frame received
  ERROR_FRAME     // ERROR received - the server closes the connection
};

/**
 * Client side of the STOMP protocol: builds frames for user commands,
 * handles frames received from the server and stores the reports.
 * Holds no socket - sending and receiving is done by the client.
 * Thread safe: called both from the keyboard thread and the socket thread.
 */
class StompProtocol {
private:
  std::mutex mutex_;        // guards all the fields below
  std::string currentUser_; // user of the current session
  int nextSubscriptionId_;  // next id for a SUBSCRIBE frame
  int nextReceiptId_; // next receipt id for a frame that requests a RECEIPT

  // channel name -> subscription id. Needed for UNSUBSCRIBE, which identifies a
  // subscription by id.
  std::unordered_map<std::string, int> channelToSubId_;

  // receipt id -> the action that requested it ("join:<channel>",
  // "exit:<channel>", "logout"). Used to know what a RECEIPT frame confirms.
  std::unordered_map<int, std::string> receiptAction_;

  // channel -> user -> reports. Used by the summary command.
  std::map<std::string, std::map<std::string, std::vector<Event>>>
      eventsByChannelAndUser_;

  /**
   * Adds a report to the stored reports, under its channel and owner user.
   *
   * @param event the report to store
   */
  void storeEvent(const Event &event);

public:
  StompProtocol();

  /**
   * Starts a new session for the given user.
   * Subscriptions and pending receipts are cleared; stored reports are kept.
   *
   * @param user name of the user that logs in
   */
  void startSession(const std::string &user);

  /**
   * Builds the CONNECT frame for the login command.
   *
   * @param user     user name
   * @param passcode password
   * @return the CONNECT frame, without the terminating '\0'
   */
  std::string buildConnectFrame(const std::string &user,
                                const std::string &passcode);

  /**
   * Registers a new subscription to the channel and builds the SUBSCRIBE frame
   * for the join command. The frame requests a RECEIPT.
   *
   * @param channel channel name
   * @param frame   set to the SUBSCRIBE frame, without the terminating '\0'
   * @return true on success, false if the user is already subscribed to the
   * channel
   */
  bool buildSubscribeFrame(const std::string &channel, std::string &frame);

  /**
   * Removes the subscription to the channel and builds the UNSUBSCRIBE frame
   * for the exit command. The frame requests a RECEIPT.
   *
   * @param channel channel name
   * @param frame   set to the UNSUBSCRIBE frame, without the terminating '\0'
   * @return true on success, false if the user is not subscribed to the channel
   */
  bool buildUnsubscribeFrame(const std::string &channel, std::string &frame);

  /**
   * Stores the events as reports of the current user and builds a SEND frame
   * for each, for the report command.
   *
   * @param nae the parsed events file
   * @return one SEND frame per event, without the terminating '\0'
   */
  std::vector<std::string> buildReportFrames(const names_and_events &nae);

  /**
   * Builds the DISCONNECT frame for the logout command. The frame requests a
   * RECEIPT.
   *
   * @return the DISCONNECT frame, without the terminating '\0'
   */
  std::string buildDisconnectFrame();

  /**
   * Writes the reports of the user in the channel to the file, for the summary
   * command. Reports are sorted by time, then by event name. The file is
   * created or overwritten.
   *
   * @param channel channel name
   * @param user    user whose reports are summarized
   * @param file    path of the output file
   * @return true on success, false if the file cannot be opened
   */
  bool writeSummary(const std::string &channel, const std::string &user,
                    const std::string &file);

  /**
   * Handles a frame received from the server: prints output to the user and
   * stores received reports. Reports sent by the current user are not stored
   * again, since they were stored when sent.
   *
   * @param rawFrame the frame, without the terminating '\0'
   * @return what the client should do with its login state and connection
   */
  FrameResult handleServerFrame(const std::string &rawFrame);
};
