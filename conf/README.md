# conf

Go package implementing INI style .conf file parsing

## API Reference

```go
// MakeConfMap constructs an empty ConfMap
func MakeConfMap() ConfMap

// MakeConfMapFromFile constructs a ConfMap filled with [Section]Key=Value elements from a file
func MakeConfMapFromFile(confFilePath string) (ConfMap, error)

// MakeConfMapFromStrings constructs a ConfMap filled with [Section]Key=Value elements from a string slice
func MakeConfMapFromStrings(confStrings []string) (confMap ConfMap, err error)

// UpdateFromString modifies a ConfMap with [Section]Key=Value elements from a string slice
func (confMap ConfMap) UpdateFromString(confString string) error

// UpdateFromString modifies a ConfMap with a [Section]Key=Value element from a string
func (confMap ConfMap) UpdateFromStrings(confStrings []string) error

// UpdateFromFile modifies a ConfMap with [Section]Key=Value elements from a file
func (confMap ConfMap) UpdateFromFile(confFilePath string) error

// FetchOptionValueStringSlice returns the Value at [Section]Key as a string slice
func (confMap ConfMap) FetchOptionValueStringSlice(sectionName string, optionName string) (optionValue []string, err error)

// FetchOptionValueString returns the Value at [Section]Key as a string
func (confMap ConfMap) FetchOptionValueString(sectionName string, optionName string) (optionValue string, err error)

// FetchOptionValueBool returns the Value at [Section]Key as a bool
func (confMap ConfMap) FetchOptionValueBool(sectionName string, optionName string) (optionValue bool, err error)

// FetchOptionValueUint16 returns the Value at [Section]Key as a uint16
func (confMap ConfMap) FetchOptionValueUint16(sectionName string, optionName string) (optionValue uint16, err error)

// FetchOptionValueUint32 returns the Value at [Section]Key as a uint32
func (confMap ConfMap) FetchOptionValueUint32(sectionName string, optionName string) (optionValue uint32, err error) {

// FetchOptionValueUint64 returns the Value at [Section]Key as a uint64
func (confMap ConfMap) FetchOptionValueUint64(sectionName string, optionName string) (optionValue uint64, err error) {

// FetchOptionValueFloatScaledToUint32 returns the Value at [Section]Key scaled via a uint32 multiplier as a uint32
func (confMap ConfMap) FetchOptionValueFloatScaledToUint32(sectionName string, optionName string, multiplier uint32) (optionValue uint32, err error)

// FetchOptionValueFloatScaledToUint64 returns the Value at [Section]Key scaled via a uint64 multiplier as a uint64
func (confMap ConfMap) FetchOptionValueFloatScaledToUint64(sectionName string, optionName string, multiplier uint64) (optionValue uint64, err error)

// FetchOptionValueDuration returns the Value at [Section]Key as a time.Duration
func (confMap ConfMap) FetchOptionValueDuration(sectionName string, optionName string) (optionValue time.Duration, err error)

// FetchOptionValueUUID returns the Value at [Section]Key as a UUID expressed as a byte slice
func (confMap ConfMap) FetchOptionValueUUID(sectionName string, optionName string) (optionValue []byte, err error)
```

## Contributors

 * ed@swiftstack.com
