// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#include <libtorrent/alert_types.hpp>
#include <libtorrent/create_torrent.hpp>
#include <libtorrent/load_torrent.hpp>
#include <libtorrent/peer_info.hpp>
#include <libtorrent/session.hpp>
#include <libtorrent/settings_pack.hpp>
#include <libtorrent/torrent_flags.hpp>
#include <libtorrent/torrent_status.hpp>

#include <array>
#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

namespace lt = libtorrent;
namespace fs = std::filesystem;

constexpr std::size_t payload_size = 2 * 1024 * 1024 + 137;
constexpr char payload_name[] = "interop-payload.bin";

unsigned char expected_byte(std::size_t offset) {
    return static_cast<unsigned char>((offset * 73) ^ (offset >> 8) ^ (offset >> 16));
}

void verify_payload(fs::path const& directory) {
    auto const path = directory / payload_name;
    if (fs::file_size(path) != payload_size) throw std::runtime_error("payload size mismatch");
    std::ifstream file(path, std::ios::binary);
    if (!file) throw std::runtime_error("cannot read payload");
    for (std::size_t offset = 0; offset < payload_size; ++offset) {
        char byte;
        if (!file.get(byte) || static_cast<unsigned char>(byte) != expected_byte(offset)) {
            throw std::runtime_error("payload byte mismatch at " + std::to_string(offset));
        }
    }
}

void prepare(fs::path const& root) {
    fs::create_directories(root / "seed");
    fs::create_directories(root / "download");
    std::ofstream file(root / "seed" / payload_name, std::ios::binary | std::ios::trunc);
    file.exceptions(std::ios::badbit | std::ios::failbit);
    for (std::size_t offset = 0; offset < payload_size; ++offset) {
        file.put(static_cast<char>(expected_byte(offset)));
    }
    file.close();
    lt::create_torrent torrent(
        std::vector<lt::create_file_entry>{{payload_name, static_cast<std::int64_t>(payload_size)}},
        16 * 1024, lt::create_torrent::v1_only);
    lt::set_piece_hashes(torrent, (root / "seed").string());
    auto const metadata = torrent.generate_buf();
    std::ofstream output(root / "interop.torrent", std::ios::binary | std::ios::trunc);
    output.exceptions(std::ios::badbit | std::ios::failbit);
    output.write(metadata.data(), static_cast<std::streamsize>(metadata.size()));
    output.close();
    verify_payload(root / "seed");
    std::cout << "{\"event\":\"prepared\",\"bytes\":" << payload_size << "}" << std::endl;
}

struct AlertSignal {
    int descriptors[2] = {-1, -1};

    AlertSignal() {
        if (::pipe(descriptors) != 0) throw std::runtime_error("cannot create alert pipe");
        for (int descriptor : descriptors) {
            if (::fcntl(descriptor, F_SETFL, O_NONBLOCK) == -1
                || ::fcntl(descriptor, F_SETFD, FD_CLOEXEC) == -1) {
                throw std::runtime_error("cannot configure alert pipe");
            }
        }
    }

    ~AlertSignal() {
        ::close(descriptors[0]);
        ::close(descriptors[1]);
    }

    void notify() const {
        char const byte = 1;
        while (::write(descriptors[1], &byte, 1) == -1 && errno == EINTR) {}
    }

    void drain() const {
        std::array<char, 256> buffer;
        while (::read(descriptors[0], buffer.data(), buffer.size()) > 0) {}
    }
};

lt::settings_pack settings() {
    lt::settings_pack options;
    options.set_str(lt::settings_pack::listen_interfaces, "127.0.0.1:0");
    options.set_str(lt::settings_pack::outgoing_interfaces, "127.0.0.1");
    options.set_bool(lt::settings_pack::enable_incoming_tcp, false);
    options.set_bool(lt::settings_pack::enable_outgoing_tcp, false);
    options.set_bool(lt::settings_pack::enable_incoming_utp, true);
    options.set_bool(lt::settings_pack::enable_outgoing_utp, true);
    options.set_bool(lt::settings_pack::enable_dht, false);
    options.set_bool(lt::settings_pack::enable_lsd, false);
    options.set_bool(lt::settings_pack::enable_upnp, false);
    options.set_bool(lt::settings_pack::enable_natpmp, false);
    options.set_bool(lt::settings_pack::listen_system_port_fallback, false);
    options.set_int(lt::settings_pack::out_enc_policy, lt::settings_pack::pe_disabled);
    options.set_int(lt::settings_pack::in_enc_policy, lt::settings_pack::pe_disabled);
    options.set_int(lt::settings_pack::alert_queue_size, 10000);
    options.set_int(lt::settings_pack::alert_mask,
        static_cast<int>(static_cast<std::uint32_t>(lt::alert_category::error | lt::alert_category::status
            | lt::alert_category::storage | lt::alert_category::connect | lt::alert_category::peer)));
    return options;
}

void run(bool seed, fs::path const& metadata, fs::path const& directory, unsigned short peer_port) {
    if (seed) verify_payload(directory);
    else if (fs::exists(directory / payload_name)) {
        throw std::runtime_error("download directory already contains payload");
    }
    fs::create_directories(directory);
    AlertSignal signal;
    lt::session session(settings());
    session.set_alert_notify([&signal] { signal.notify(); });
    auto parameters = lt::load_torrent_file(metadata.string());
    if (parameters.ti->num_files() != 1
        || parameters.ti->layout().file_path(lt::file_index_t{0}) != payload_name
        || parameters.ti->total_size() != static_cast<std::int64_t>(payload_size)) {
        throw std::runtime_error("metadata does not describe the prepared fixture");
    }
    if (!parameters.trackers.empty() || !parameters.url_seeds.empty()) {
        throw std::runtime_error("fixture must not contain trackers or web seeds");
    }
    parameters.save_path = directory.string();
    parameters.flags &= ~(lt::torrent_flags::paused | lt::torrent_flags::auto_managed);
    parameters.trackers.clear();
    parameters.url_seeds.clear();
    parameters.peers.clear();
    auto torrent = session.add_torrent(parameters);
    bool ready = false;
    bool flushing = false;
    bool verified = false;
    bool finish_requested = false;
    bool input_open = true;
    unsigned short listen_port = 0;
    unsigned int utp_connections = 0;
    unsigned int incoming_connections = 0;
    unsigned int outgoing_connections = 0;
    std::string input;

    for (;;) {
        std::vector<lt::alert*> alerts;
        session.pop_alerts(&alerts);
        bool flushed = false;
        for (auto const* alert : alerts) {
            if (auto const* connected = lt::alert_cast<lt::peer_connect_alert>(alert)) {
                if (connected->socket_type != lt::socket_type_t::utp) {
                    throw std::runtime_error("non-uTP peer connection observed");
                }
                ++utp_connections;
                bool const incoming = connected->direction == lt::peer_connect_alert::direction_t::in;
                if (incoming) ++incoming_connections;
                else ++outgoing_connections;
                std::cout << "{\"event\":\"peer\",\"transport\":\"utp\",\"direction\":\""
                    << (incoming ? "incoming" : "outgoing") << "\"}" << std::endl;
            }
            if (auto const* listened = lt::alert_cast<lt::listen_succeeded_alert>(alert)) {
                if (listened->socket_type == lt::socket_type_t::utp) {
                    listen_port = static_cast<unsigned short>(listened->port);
                }
            }
            if (lt::alert_cast<lt::listen_failed_alert>(alert)
                || lt::alert_cast<lt::torrent_error_alert>(alert)
                || lt::alert_cast<lt::file_error_alert>(alert)
                || lt::alert_cast<lt::alerts_dropped_alert>(alert)) {
                throw std::runtime_error(alert->message());
            }
            if (auto const* cache = lt::alert_cast<lt::cache_flushed_alert>(alert)) {
                if (flushing && cache->handle == torrent) flushed = true;
            }
            if (lt::alert_cast<lt::state_changed_alert>(alert)
                || lt::alert_cast<lt::piece_finished_alert>(alert)
                || lt::alert_cast<lt::peer_error_alert>(alert)
                || lt::alert_cast<lt::peer_disconnected_alert>(alert)) {
                std::cerr << alert->message() << std::endl;
            }
        }

        auto const status = torrent.status();
        if (status.errc) throw std::runtime_error(status.errc.message());
        std::vector<lt::peer_info> peers;
        torrent.get_peer_info(peers);
        for (auto const& peer : peers) {
            if (!(peer.flags & lt::peer_info::utp_socket)) {
                throw std::runtime_error("non-uTP peer present in peer snapshot");
            }
        }
        bool const checked = status.state != lt::torrent_status::checking_files
            && status.state != lt::torrent_status::checking_resume_data;
        if (!ready && listen_port != 0 && checked && (!seed || status.is_seeding)) {
            ready = true;
            std::cout << "{\"event\":\"ready\",\"mode\":\"" << (seed ? "seed" : "download")
                << "\",\"port\":" << listen_port << ",\"utp_only\":true}" << std::endl;
            if (peer_port != 0) {
                torrent.connect_peer({lt::make_address("127.0.0.1"), peer_port});
            }
        }
        if (!flushing && ready && ((seed && finish_requested) || (!seed && status.is_seeding))) {
            if (utp_connections == 0) throw std::runtime_error("completion without a uTP peer");
            if (seed ? status.total_payload_upload == 0 : status.total_payload_download == 0) {
                throw std::runtime_error("completion without transferred payload");
            }
            flushing = true;
            torrent.flush_cache();
        }
        if (flushed && !verified) {
            verify_payload(directory);
            verified = true;
            if (!seed) {
                std::cout << "{\"event\":\"downloaded\",\"verified_bytes\":" << payload_size << "}" << std::endl;
            }
        }
        if (verified && finish_requested) {
            std::cout << "{\"event\":\"complete\",\"verified_bytes\":" << payload_size
                << ",\"utp_connections\":" << utp_connections << ",\"tcp_connections\":0"
                << ",\"incoming_connections\":" << incoming_connections
                << ",\"outgoing_connections\":" << outgoing_connections
                << ",\"payload_download\":" << status.total_payload_download
                << ",\"payload_upload\":" << status.total_payload_upload << "}" << std::endl;
            return;
        }

        pollfd descriptors[2] = {
            {signal.descriptors[0], POLLIN, 0},
            {input_open ? STDIN_FILENO : -1, POLLIN, 0},
        };
        int result;
        do { result = ::poll(descriptors, 2, -1); } while (result == -1 && errno == EINTR);
        if (result == -1) throw std::runtime_error("cannot wait for helper events");
        if (descriptors[0].revents & POLLIN) signal.drain();
        if (descriptors[1].revents & (POLLIN | POLLHUP)) {
            std::array<char, 256> buffer;
            auto const count = ::read(STDIN_FILENO, buffer.data(), buffer.size());
            if (count < 0) throw std::runtime_error("cannot read helper control");
            if (count == 0) {
                input_open = false;
                if (!finish_requested) throw std::runtime_error("control closed before finish");
            } else {
                input.append(buffer.data(), static_cast<std::size_t>(count));
                for (auto newline = input.find('\n'); newline != std::string::npos; newline = input.find('\n')) {
                    auto const command = input.substr(0, newline);
                    input.erase(0, newline + 1);
                    if (command != "finish" || !ready || (!seed && !verified)) {
                        throw std::runtime_error("unexpected helper control command");
                    }
                    finish_requested = true;
                }
                if (input.size() > 32) throw std::runtime_error("oversized helper control command");
            }
        }
    }
}

int main(int argc, char** argv) {
    try {
        if (argc == 3 && std::string(argv[1]) == "prepare") prepare(argv[2]);
        else if (argc == 3 && std::string(argv[1]) == "verify") {
            verify_payload(argv[2]);
            std::cout << "{\"event\":\"verified\",\"bytes\":" << payload_size << "}" << std::endl;
        } else if ((argc == 4 || argc == 5)
            && (std::string(argv[1]) == "seed" || std::string(argv[1]) == "download")) {
            unsigned long port = 0;
            if (argc == 5) {
                std::size_t consumed = 0;
                port = std::stoul(argv[4], &consumed);
                if (consumed != std::string(argv[4]).size() || port == 0 || port > 65535) {
                    throw std::runtime_error("peer port must be in 1..65535");
                }
            }
            run(std::string(argv[1]) == "seed", argv[2], argv[3], static_cast<unsigned short>(port));
        } else {
            std::cerr << "usage: interop prepare ROOT | verify DATA_DIR | seed|download TORRENT DATA_DIR [PEER_PORT]\n";
            return 2;
        }
        return 0;
    } catch (std::exception const& error) {
        std::cerr << "ERROR: " << error.what() << std::endl;
        return 1;
    }
}
