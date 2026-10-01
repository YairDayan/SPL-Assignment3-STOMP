#pragma once

#include <boost/asio.hpp>
#include <iostream>
#include <string>

using boost::asio::ip::tcp;

/**
 * Owns the TCP socket between the client and the server.
 * Knows only how to send and receive bytes; it has no knowledge of STOMP.
 */
class ConnectionHandler {
private:
  const std::string host_;             // Server IP address
  const short port_;                   // Server port
  boost::asio::io_service io_service_; // Provides core I/O functionality
  tcp::socket socket_;                 // The TCP socket connected to the server

public:
  /**
   * Creates a handler for a connection to the given server.
   * Only stores the address; the socket is not connected until connect() is
   * called.
   *
   * @param host server IP address
   * @param port server port
   */
  ConnectionHandler(std::string host, short port);

  /**
   * Closes the socket when the handler is destroyed.
   */
  virtual ~ConnectionHandler();

  /**
   * Opens a TCP connection to the server.
   *
   * @return true if the connection was established, false otherwise
   */
  bool connect();

  /**
   * Reads exactly bytesToRead bytes from the server. Blocks until all of them
   * arrive.
   *
   * @param bytes       buffer to fill
   * @param bytesToRead number of bytes to read
   * @return true on success, false if the connection was closed before all
   * bytes were read
   */
  bool getBytes(char bytes[], unsigned int bytesToRead);

  /**
   * Sends exactly bytesToWrite bytes to the server. Blocks until all of them
   * are sent.
   *
   * @param bytes        data to send
   * @param bytesToWrite number of bytes to send
   * @return true on success, false if the connection was closed before all
   * bytes were sent
   */
  bool sendBytes(const char bytes[], int bytesToWrite);

  /**
   * Reads a line from the server, up to and including '\n'. Blocks until the
   * line is complete.
   *
   * @param line string to append the received line to
   * @return true on success, false if the connection was closed before '\n' was
   * read
   */
  bool getLine(std::string &line);

  /**
   * Sends a line to the server, followed by '\n'.
   *
   * @param line the line to send, without '\n'
   * @return true on success, false if the connection was closed before all data
   * was sent
   */
  bool sendLine(std::string &line);

  /**
   * Reads from the server until the delimiter character. Blocks until it
   * arrives. With '\0' as the delimiter, this reads one complete STOMP frame.
   *
   * @param frame     string to append the received data to; '\0' is never
   * appended
   * @param delimiter character that ends the frame
   * @return true on success, false if the connection was closed before the
   * delimiter was read
   */
  bool getFrameAscii(std::string &frame, char delimiter);

  /**
   * Sends the frame to the server, followed by the delimiter character.
   * With '\0' as the delimiter, this sends one complete STOMP frame.
   *
   * @param frame     the data to send, without the delimiter
   * @param delimiter character that ends the frame
   * @return true on success, false if the connection was closed before all data
   * was sent
   */
  bool sendFrameAscii(const std::string &frame, char delimiter);

  /**
   * Closes the socket. A thread blocked in getFrameAscii wakes up and gets
   * false. Closing an already closed socket is ignored.
   */
  void close();

}; // class ConnectionHandler
