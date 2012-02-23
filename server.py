#!/usr/bin/env python

import os
import sys
import libtorrent as lt
import time
import urllib
import random

import pymongo


class MagnetDaemon:

    def __init__(self):
        self.session = lt.session()
        # self.session.listen_on(8888, 9999)
        self.session.start_dht()
        # self.session.stop_upnp()
        # self.session.stop_natpmp()
        self.mongconn = pymongo.Connection()
        self.db = self.mongconn['magnet']
        self.peers = self.db.peers
        self.hashes = self.db.hashes
        self.handle_ages = {}

    def create_handle(self, link=None, path=None):
        params = {
            'save_path': 'tmp',
            # 'upload_mode': True,
            'storage_mode': lt.storage_mode_t.storage_mode_compact,
            'paused': False,
            'auto_managed': False,  # Disables queueing
        }
        if link:
            h = lt.add_magnet_uri(self.session, link, params)
        if path:
            h = lt.add_files(self.session, path, params)

        return h

    def create_magnet_url(self, info_hash):
        trackers = [
            'http://tracker.openbittorent.org/announce',
            'http://tracker.publicbt.org/announce',
        ]

        s = 'magnet:?xt=urn:btih:%s' % info_hash
        for t in trackers:
            s += '&tr='
            s += urllib.quote(t)
        return str(s)

    def add_from_hash(self, info_hash):
        link = self.create_magnet_url(info_hash)
        handle = self.create_handle(link=link)

    def add_hashes_from_dump(self, path):
        '''Immediately add to the session'''
        print 'Reading "%s"' % path
        for no, line in enumerate(open(path)):
            line = line.strip()
            info_hash = self.hash_from_line(line)
            self.add_from_hash(info_hash)
        print 'Done'

    def load_hashes_from_dump(self, path):
        '''Load into mongodb'''
        print 'Reading "%s"' % path
        no = 0
        for line in open(path):
            no += 1
            if no % 10000 == 0:
                print no
            line = line.strip()
            info_hash = self.hash_from_line(line)
            d = {'hash': str(info_hash)}
            dbhash = self.hashes.find_one(d)
            if not dbhash:
                print 'insert'
                self.hashes.insert(d)
        print 'Done'

    def hash_from_line(self, line):
        h = line.split('|')
        return h[-1]

    def remove(self, handle):
        del self.handle_ages[str(handle.info_hash())]
        self.session.remove_torrent(handle)

    def monitor(self):
        for torrent in self.session.get_torrents():

            print 'S', torrent.status().num_seeds,
            print 'P', torrent.status().list_peers,
            print torrent.name(),
            print torrent.status().current_tracker,
            print

            info_hash = str(torrent.info_hash())
            if info_hash not in self.handle_ages:
                self.handle_ages[info_hash] = time.time()

            if time.time() - self.handle_ages[info_hash] > 20:
                self.remove(torrent)
                print ' -> REMOVED'
                continue

            if not torrent.has_metadata():
                continue
            self.add_peers_to_db(torrent)

            # Now remove this torrent so we can leave room for others
            self.remove(torrent)
            return

        print 80 * '-'

    def run(self):
        while 1:
            self.add_random_torrents(50)
            self.monitor()
            time.sleep(10)

    def add_random_torrents(self, limit):
        hashes = None
        while len(self.session.get_torrents()) < limit:

            # XXX: probably memory/time hog
            if not hashes:
                hashes = list(self.hashes.find())

            c = random.choice(hashes)
            self.add_from_hash(c['hash'])

    def add_peers_to_db(self, torrent):
        peers = torrent.get_peer_info()
        print len(peers), torrent.name()

        for p in peers:
            print ' - %s %s' % (p.ip, p.client)

            addr = p.ip[0]
            peer = self.peers.find_one({'addr': addr})
            if not peer:
                peer = {
                    'addr': addr,
                    'torrents': []
                    }

            peer['client'] = p.client
            ih = str(torrent.info_hash())
            if ih not in peer['torrents']:
                peer['torrents'].append(ih)
            self.peers.save(peer)


def main():
    md = MagnetDaemon()
    #md.add_from_hash('2ded86af7c0cce8224262ba703191bf7d8537a5d')
    #md.add_from_hash('368a8beaaced1572f44ad5aae0685ed6130d983c')
    # md.load_hashes_from_dump('complete')
    #md.run()

if __name__ == '__main__':
    main()

