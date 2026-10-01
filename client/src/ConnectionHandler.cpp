#include "../include/ConnectionHandler.h"

using boost::asio::ip::tcp;
using std::string;

ConnectionHandler::ConnectionHandler(string host, short port)
    : host_(host), port_(port), io_service_(), socket_(io_service_) {}

ConnectionHandler::~ConnectionHandler() { close(); }

bool ConnectionHandler::connect() {
  try {
    tcp::endpoint endpoint(boost::asio::ip::address::from_string(host_), port_);
    socket_.connect(endpoint);
  } catch (std::exception &e) {
    return false;
  }
  return true;
}

bool ConnectionHandler::getBytes(char bytes[], unsigned int bytesToRead) {
  size_t tmp = 0;
  boost::system::error_code error;
  try {
    // read_some may return fewer bytes than requested, so keep reading until
    // all bytes arrived.
    while (!error && bytesToRead > tmp) {
      tmp += socket_.read_some(
          boost::asio::buffer(bytes + tmp, bytesToRead - tmp), error);
    }
    if (error)
      throw boost::system::system_error(error);
  } catch (std::exception &e) {
    return false;
  }
  return true;
}

bool ConnectionHandler::sendBytes(const char bytes[], int bytesToWrite) {
  int tmp = 0;
  boost::system::error_code error;
  try {
    // write_some may send fewer bytes than requested, so keep writing until all
    // bytes are sent.
    while (!error && bytesToWrite > tmp) {
      tmp += socket_.write_some(
          boost::asio::buffer(bytes + tmp, bytesToWrite - tmp), error);
    }
    if (error)
      throw boost::system::system_error(error);
  } catch (std::exception &e) {
    return false;
  }
  return true;
}

bool ConnectionHandler::getLine(std::string &line) {
  return getFrameAscii(line, '\n');
}

bool ConnectionHandler::sendLine(std::string &line) {
  return sendFrameAscii(line, '\n');
}

bool ConnectionHandler::getFrameAscii(std::string &frame, char delimiter) {
  char ch;
  try {
    // Read one byte at a time, so that no bytes of the next frame are consumed.
    do {
      if (!getBytes(&ch, 1)) {
        return false;
      }
      if (ch != '\0')
        frame.append(1, ch);
    } while (delimiter != ch);
  } catch (std::exception &e) {
    return false;
  }
  return true;
}

bool ConnectionHandler::sendFrameAscii(const std::string &frame,
                                       char delimiter) {
  bool result = sendBytes(frame.c_str(), frame.length());
  if (!result)
    return false;
  return sendBytes(&delimiter, 1);
}

void ConnectionHandler::close() {
  // shutdown wakes up a thread blocked in a read on this socket; close alone
  // does not on Linux.
  boost::system::error_code ignored;
  socket_.shutdown(tcp::socket::shutdown_both, ignored);
  try {
    socket_.close();
  } catch (...) {
  }
}
