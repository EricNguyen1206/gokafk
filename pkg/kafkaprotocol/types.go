package kafkaprotocol

const (
	ApiKeyProduce         int16 = 0
	ApiKeyFetch           int16 = 1
	ApiKeyListOffsets     int16 = 2
	ApiKeyMetadata        int16 = 3
	ApiKeyOffsetCommit    int16 = 8
	ApiKeyOffsetFetch     int16 = 9
	ApiKeyFindCoordinator int16 = 10
	ApiKeyJoinGroup       int16 = 11
	ApiKeyHeartbeat       int16 = 12
	ApiKeyLeaveGroup      int16 = 13
	ApiKeySyncGroup       int16 = 14
	ApiKeyApiVersions     int16 = 18
)
