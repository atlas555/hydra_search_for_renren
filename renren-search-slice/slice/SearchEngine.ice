#ifndef _SEARCHENGINEINTERFACE_ICE
#define _SEARCHENGINEINTERFACE_ICE

//#include <Util.ice>

module com {
	module renren {
		module hydra {
			sequence<byte> ByteSeq;

			interface Broker {
				ByteSeq search(ByteSeq condition, int offset, int limit);
			};

			interface Searcher {
				ByteSeq search(ByteSeq req, int count);
			};

		};
	};
};

#endif

