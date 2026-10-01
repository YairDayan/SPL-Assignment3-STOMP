#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <chrono>

#include "../include/ConnectionHandler.h"
#include "../include/StompProtocol.h"
#include "../include/event.h"

/**
 * Splits a command line into words separated by whitespace.
 *
 * @param line the command line
 * @return the words, in order
 */
static std::vector<std::string> splitWS(const std::string& line) {
    std::vector<std::string> out;
    std::istringstream iss(line);
    std::string tok;
    while (iss >> tok) out.push_back(tok);
    return out;
}

/**
 * Entry point of the client.
 * The main thread reads commands from the keyboard and sends frames; while logged in,
 * a second thread (the reader) reads frames from the socket and handles them.
 */
int main(int argc, char *argv[]) {
    StompProtocol protocol;
    std::unique_ptr<ConnectionHandler> connection;
    std::thread readerThread;

    std::atomic<bool> loggedIn(false);
    std::atomic<bool> stopReader(false);              // tells the reader thread to exit its loop
    std::atomic<bool> awaitingLoginResponse(false);   // the keyboard thread waits for CONNECTED / ERROR
    std::atomic<bool> awaitingLogoutReceipt(false);   // the keyboard thread waits for the DISCONNECT RECEIPT

    // Used by the keyboard thread to wait (up to 5 seconds) until the reader thread
    // clears awaitingLoginResponse / awaitingLogoutReceipt.
    std::mutex waitMutex;
    std::condition_variable loginCv;
    std::condition_variable logoutCv;

    // Locking waitMutex before notifying guarantees the waiting thread is either
    // already waiting or has not checked its condition yet, so the notification is not lost.
    auto notify = [&](std::condition_variable& cv) {
        std::lock_guard<std::mutex> lock(waitMutex);
        cv.notify_all();
    };

    auto closeConnection = [&]() {
        if (connection) connection->close();
    };

    // Returns false if there is no connection or the send failed.
    auto sendFrame = [&](const std::string& frame) -> bool {
        if (!connection) return false;
        return connection->sendFrameAscii(frame, '\0');
    };

    // Closing the socket unblocks the reader thread if it is waiting for a frame.
    auto stopAndJoinReader = [&]() {
        stopReader.store(true);
        closeConnection();
        if (readerThread.joinable()) readerThread.join();
    };

    // Runs on the reader thread: updates the login state according to the frame.
    auto handleServerFrame = [&](const std::string& rawFrame) {
        FrameResult result = protocol.handleServerFrame(rawFrame);

        if (result == FrameResult::LOGIN_SUCCESS) {
            loggedIn.store(true);
            if (awaitingLoginResponse.exchange(false)) {
                notify(loginCv);
            }
        } else if (result == FrameResult::LOGOUT_RECEIPT) {
            awaitingLogoutReceipt.store(false);
            loggedIn.store(false);
            stopReader.store(true);
            closeConnection();
            notify(logoutCv);
        } else if (result == FrameResult::ERROR_FRAME) {
            // The server closes the connection after an ERROR frame.
            awaitingLogoutReceipt.store(false);
            loggedIn.store(false);
            if (awaitingLoginResponse.exchange(false)) {
                notify(loginCv);
            }
            stopReader.store(true);
            closeConnection();
            notify(logoutCv);
        }
    };

    auto startReader = [&]() {
        // The previous reader may have exited by itself (ERROR frame or server closed the socket)
        // without being joined; assigning to a joinable std::thread would terminate the program.
        if (readerThread.joinable()) readerThread.join();
        stopReader.store(false);

        readerThread = std::thread([&]() {
            while (!stopReader.load()) {
                if (!connection) break;
                std::string frame;
                if (!connection->getFrameAscii(frame, '\0')) break;
                handleServerFrame(frame);
            }

            // The connection is gone: release the keyboard thread if it is waiting.
            if (loggedIn.load()) {
                loggedIn.store(false);
                awaitingLogoutReceipt.store(false);
                notify(logoutCv);
            }
            if (awaitingLoginResponse.exchange(false)) {
                notify(loginCv);
            }
        });
    };

    std::string line;
    while (std::getline(std::cin, line)) {
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

            // The old reader must be finished before its connection object is replaced.
            if (readerThread.joinable()) readerThread.join();
            connection = std::move(newConn);
            protocol.startSession(args[2]);

            // Set before sending, so a fast reply is not missed.
            awaitingLoginResponse.store(true);
            startReader();

            if (!sendFrame(protocol.buildConnectFrame(args[2], args[3]))) {
                std::cout << "Could not connect to server" << std::endl;
                awaitingLoginResponse.store(false);
                stopAndJoinReader();
                continue;
            }

            // "Login successful" or the error is printed by the reader thread.
            bool gotLoginResponse;
            {
                std::unique_lock<std::mutex> lk(waitMutex);
                gotLoginResponse = loginCv.wait_for(
                    lk,
                    std::chrono::seconds(5),
                    [&]() { return !awaitingLoginResponse.load(); }
                );
            }
            // Joining is done after releasing waitMutex, since the reader locks it to notify.
            if (!gotLoginResponse) {
                awaitingLoginResponse.store(false);
                std::cout << "Could not connect to server" << std::endl;
                stopAndJoinReader();
            }
            continue;
        }

        // All commands except login require a logged-in user.
        if (!loggedIn.load()) {
            std::cout << "Please login first" << std::endl;
            continue;
        }

        if (cmd == "join") {
            if (args.size() != 2) {
                std::cout << "Usage: join {channel_name}" << std::endl;
                continue;
            }
            std::string frame;
            if (!protocol.buildSubscribeFrame(args[1], frame)) {
                std::cout << "Already joined channel " << args[1] << std::endl;
                continue;
            }
            sendFrame(frame);
            continue;
        }

        if (cmd == "exit") {
            if (args.size() != 2) {
                std::cout << "Usage: exit {channel_name}" << std::endl;
                continue;
            }

            std::string frame;
            if (!protocol.buildUnsubscribeFrame(args[1], frame)) {
                std::cout << "Not subscribed to channel " << args[1] << std::endl;
                continue;
            }
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

            std::vector<std::string> frames = protocol.buildReportFrames(nae);
            for (std::vector<std::string>::const_iterator it = frames.begin(); it != frames.end(); ++it) {
                sendFrame(*it);
            }
            continue;
        }

        if (cmd == "summary") {
            if (args.size() != 4) {
                std::cout << "Usage: summary {channel_name} {user} {file}" << std::endl;
                continue;
            }

            if (!protocol.writeSummary(args[1], args[2], args[3])) {
                std::cout << "Failed to write summary file" << std::endl;
            }
            continue;
        }

        if (cmd == "logout") {
            awaitingLogoutReceipt.store(true);

            sendFrame(protocol.buildDisconnectFrame());

            // Graceful shutdown: the socket is closed only after the RECEIPT arrives (or after a timeout).
            bool gotReceipt;
            {
                std::unique_lock<std::mutex> lk(waitMutex);
                gotReceipt = logoutCv.wait_for(
                    lk,
                    std::chrono::seconds(5),
                    [&]() { return !awaitingLogoutReceipt.load(); }
                );
            }

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
