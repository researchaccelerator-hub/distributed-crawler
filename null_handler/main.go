package null_handler

import (
	"encoding/json"
	"fmt"

	"reflect"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/rs/zerolog/log"
)

// Platform represents the different platforms
type Platform string

const (
	PlatformTelegram Platform = "telegram"
	PlatformYouTube  Platform = "youtube"
)

// ValidationBehavior defines how to handle null/empty values
type ValidationBehavior string

const (
	BehaviorCritical    ValidationBehavior = "critical"    // Skip processing if null
	BehaviorLog         ValidationBehavior = "log"         // Log warning if null
	BehaviorUnavailable ValidationBehavior = "unavailable" // Field not available for platform
	BehaviorOptional    ValidationBehavior = "optional"    // No action needed
)

// FieldConfig defines validation behavior for a specific field
type FieldConfig struct {
	Behavior ValidationBehavior `json:"behavior"`
	Message  string             `json:"message"` // Custom message for logging
}

// ValidationConfig holds validation rules for each platform
type ValidationConfig struct {
	Platform Platform               `json:"platform"`
	Rules    map[string]FieldConfig `json:"rules"` // field path -> config
}

// UserValidationConfig allows users to submit partial configs
type UserValidationConfig struct {
	Platform Platform               `json:"platform"`
	Rules    map[string]FieldConfig `json:"rules"`
}

// ValidationResult holds the validation outcome
type ValidationResult struct {
	Valid           bool
	Errors          []string
	Warnings        []string
	UnavailableUsed []string
	NullLogEvents   []NullLogEvent
}

// NullLogEvent for monitoring nulls in Channel and Post data
type NullLogEvent struct {
	Platform        string `json:"platform"`
	DataType        string `json:"data_type"` // e.g., "channel", "post"
	FieldName       string `json:"field_name"`
	StrategyUsed    string `json:"strategy_used"`
	IsPlatformLimit bool   `json:"is_platform_limit"`
	Message         string `json:"message"`
}

// DefaultConfigs provides platform-specific validation configurations
func DefaultConfigs() map[Platform]*ValidationConfig {
	return map[Platform]*ValidationConfig{
		PlatformYouTube: {
			Platform: PlatformYouTube,
			Rules: map[string]FieldConfig{
				// ChannelData fields
				"ChannelData.ChannelID":                            {Behavior: BehaviorCritical, Message: "ChannelID is required"},
				"ChannelData.ChannelName":                          {Behavior: BehaviorCritical, Message: "ChannelName is required"},
				"ChannelData.ChannelDescription":                   {Behavior: BehaviorLog, Message: "ChannelDescription is empty"},
				"ChannelData.ChannelProfileImage":                  {Behavior: BehaviorLog, Message: "ChannelProfileImage is empty"},
				"ChannelData.ChannelEngagementData.FollowerCount":  {Behavior: BehaviorLog, Message: "FollowerCount is zero"},
				"ChannelData.ChannelEngagementData.FollowingCount": {Behavior: BehaviorUnavailable, Message: "FollowingCount not available on YouTube"},
				"ChannelData.ChannelEngagementData.LikeCount":      {Behavior: BehaviorUnavailable, Message: "LikeCount not available on YouTube"},
				"ChannelData.ChannelEngagementData.PostCount":      {Behavior: BehaviorLog, Message: "PostCount is zero"},
				"ChannelData.ChannelEngagementData.ViewsCount":     {Behavior: BehaviorLog, Message: "ViewsCount is zero"},
				"ChannelData.ChannelEngagementData.CommentCount":   {Behavior: BehaviorUnavailable, Message: "CommentCount not available on YouTube"},
				"ChannelData.ChannelEngagementData.ShareCount":     {Behavior: BehaviorUnavailable, Message: "ShareCount not available on YouTube"},
				"ChannelData.ChannelURLExternal":                   {Behavior: BehaviorLog, Message: "ChannelURLExternal is empty"},
				"ChannelData.ChannelURL":                           {Behavior: BehaviorCritical, Message: "ChannelURL is required"},
				"ChannelData.CountryCode":                          {Behavior: BehaviorOptional, Message: "CountryCode is empty"},
				"ChannelData.PublishedAt":                          {Behavior: BehaviorLog, Message: "PublishedAt is zero"},

				// Post fields
				"PostLink":                   {Behavior: BehaviorCritical, Message: "PostLink is required"},
				"ChannelID":                  {Behavior: BehaviorCritical, Message: "ChannelID is required"},
				"PostUID":                    {Behavior: BehaviorCritical, Message: "PostUID is required"},
				"URL":                        {Behavior: BehaviorCritical, Message: "URL is required"},
				"PublishedAt":                {Behavior: BehaviorCritical, Message: "PublishedAt is required"},
				"CreatedAt":                  {Behavior: BehaviorLog, Message: "CreatedAt is zero"},
				"LanguageCode":               {Behavior: BehaviorLog, Message: "LanguageCode is empty"},
				"Engagement":                 {Behavior: BehaviorLog, Message: "Engagement is zero"},
				"ViewCount":                  {Behavior: BehaviorLog, Message: "ViewCount is zero"},
				"LikeCount":                  {Behavior: BehaviorLog, Message: "LikeCount is zero"},
				"ShareCount":                 {Behavior: BehaviorUnavailable, Message: "ShareCount not available on YouTube"},
				"CommentCount":               {Behavior: BehaviorLog, Message: "CommentCount is zero"},
				"CrawlLabel":                 {Behavior: BehaviorLog, Message: "CrawlLabel is empty"},
				"ListIDs":                    {Behavior: BehaviorUnavailable, Message: "ListIDs is empty"},
				"ChannelName":                {Behavior: BehaviorLog, Message: "ChannelName is empty"},
				"SearchTerms":                {Behavior: BehaviorUnavailable, Message: "SearchTerms is empty"},
				"SearchTermIDs":              {Behavior: BehaviorUnavailable, Message: "SearchTermIDs is empty"},
				"ProjectIDs":                 {Behavior: BehaviorUnavailable, Message: "ProjectIDs is empty"},
				"ExerciseIDs":                {Behavior: BehaviorUnavailable, Message: "ExerciseIDs is empty"},
				"LabelData":                  {Behavior: BehaviorUnavailable, Message: "LabelData is empty"},
				"LabelsMetadata":             {Behavior: BehaviorUnavailable, Message: "LabelsMetadata is empty"},
				"ProjectLabeledPostIDs":      {Behavior: BehaviorUnavailable, Message: "ProjectLabeledPostIDs is empty"},
				"LabelerIDs":                 {Behavior: BehaviorUnavailable, Message: "LabelerIDs is empty"},
				"AllLabels":                  {Behavior: BehaviorUnavailable, Message: "AllLabels is empty"},
				"LabelIDs":                   {Behavior: BehaviorUnavailable, Message: "LabelIDs is empty"},
				"IsAd":                       {Behavior: BehaviorUnavailable, Message: "IsAd is false"},
				"TranscriptText":             {Behavior: BehaviorUnavailable, Message: "TranscriptText is empty"},
				"ImageText":                  {Behavior: BehaviorUnavailable, Message: "ImageText is empty"},
				"VideoLength":                {Behavior: BehaviorLog, Message: "VideoLength is null"},
				"IsVerified":                 {Behavior: BehaviorUnavailable, Message: "IsVerified is null"},
				"PlatformName":               {Behavior: BehaviorCritical, Message: "PlatformName is required"},
				"SharedID":                   {Behavior: BehaviorUnavailable, Message: "SharedID is null"},
				"QuotedID":                   {Behavior: BehaviorUnavailable, Message: "QuotedID is null"},
				"RepliedID":                  {Behavior: BehaviorUnavailable, Message: "RepliedID is null"},
				"AILabel":                    {Behavior: BehaviorUnavailable, Message: "AILabel is null"},
				"RootPostID":                 {Behavior: BehaviorUnavailable, Message: "RootPostID is null"},
				"EngagementStepsCount":       {Behavior: BehaviorUnavailable, Message: "EngagementStepsCount is zero"},
				"OCRData":                    {Behavior: BehaviorLog, Message: "OCRData is empty"},
				"PerformanceScores.Likes":    {Behavior: BehaviorLog, Message: "PerformanceScores.Likes is null"},
				"PerformanceScores.Shares":   {Behavior: BehaviorUnavailable, Message: "PerformanceScores.Shares not available on YouTube"},
				"PerformanceScores.Comments": {Behavior: BehaviorLog, Message: "PerformanceScores.Comments is null"},
				"PerformanceScores.Views":    {Behavior: BehaviorLog, Message: "PerformanceScores.Views is zero"},
				"HasEmbedMedia":              {Behavior: BehaviorLog, Message: "HasEmbedMedia is null"},
				"Description":                {Behavior: BehaviorLog, Message: "Description is empty"},
				"RepostChannelData":          {Behavior: BehaviorUnavailable, Message: "RepostChannelData is null"},
				"PostType":                   {Behavior: BehaviorLog, Message: "PostType is empty"},
				"InnerLink":                  {Behavior: BehaviorUnavailable, Message: "InnerLink is empty"},
				"PostTitle":                  {Behavior: BehaviorLog, Message: "PostTitle is null"},
				"MediaData.DocumentName":     {Behavior: BehaviorLog, Message: "MediaData.DocumentName is empty"},
				"IsReply":                    {Behavior: BehaviorUnavailable, Message: "IsReply is null"},
				"AdFields":                   {Behavior: BehaviorUnavailable, Message: "AdFields is null"},
				"LikesCount":                 {Behavior: BehaviorLog, Message: "LikesCount is zero"},
				"SharesCount":                {Behavior: BehaviorUnavailable, Message: "SharesCount not available on YouTube"},
				"CommentsCount":              {Behavior: BehaviorLog, Message: "CommentsCount is zero"},
				"ViewsCount":                 {Behavior: BehaviorLog, Message: "ViewsCount is zero"},
				"SearchableText":             {Behavior: BehaviorLog, Message: "SearchableText is empty"},
				"AllText":                    {Behavior: BehaviorLog, Message: "AllText is empty"},
				"ContrastAgentProjectIDs":    {Behavior: BehaviorUnavailable, Message: "ContrastAgentProjectIDs is empty"},
				"AgentIDs":                   {Behavior: BehaviorUnavailable, Message: "AgentIDs is empty"},
				"SegmentIDs":                 {Behavior: BehaviorUnavailable, Message: "SegmentIDs is empty"},
				"ThumbURL":                   {Behavior: BehaviorLog, Message: "ThumbURL is empty"},
				"MediaURL":                   {Behavior: BehaviorLog, Message: "MediaURL is empty"},
				"Comments":                   {Behavior: BehaviorUnavailable, Message: "Comments is empty"},
				"Reactions":                  {Behavior: BehaviorLog, Message: "Reactions not available on YouTube"},
				"Outlinks":                   {Behavior: BehaviorLog, Message: "Outlinks is empty"},
				"CaptureTime":                {Behavior: BehaviorLog, Message: "CaptureTime is zero"},
				"Handle":                     {Behavior: BehaviorLog, Message: "Handle is empty"},
			},
		},
		PlatformTelegram: {
			Platform: PlatformTelegram,
			Rules: map[string]FieldConfig{
				// ChannelData fields
				"ChannelData.ChannelID":                            {Behavior: BehaviorCritical, Message: "ChannelID is required"},
				"ChannelData.ChannelName":                          {Behavior: BehaviorCritical, Message: "ChannelName is required"},
				"ChannelData.ChannelDescription":                   {Behavior: BehaviorLog, Message: "ChannelDescription is empty"},
				"ChannelData.ChannelProfileImage":                  {Behavior: BehaviorLog, Message: "ChannelProfileImage is empty"},
				"ChannelData.ChannelEngagementData.FollowerCount":  {Behavior: BehaviorLog, Message: "FollowerCount is zero"},
				"ChannelData.ChannelEngagementData.FollowingCount": {Behavior: BehaviorUnavailable, Message: "FollowingCount not available on Telegram"},
				"ChannelData.ChannelEngagementData.LikeCount":      {Behavior: BehaviorUnavailable, Message: "LikeCount not available on Telegram"},
				"ChannelData.ChannelEngagementData.PostCount":      {Behavior: BehaviorLog, Message: "PostCount is zero"},
				"ChannelData.ChannelEngagementData.ViewsCount":     {Behavior: BehaviorLog, Message: "ViewsCount is zero"},
				"ChannelData.ChannelEngagementData.CommentCount":   {Behavior: BehaviorUnavailable, Message: "CommentCount not available on Telegram"},
				"ChannelData.ChannelEngagementData.ShareCount":     {Behavior: BehaviorUnavailable, Message: "ShareCount is zero"},
				"ChannelData.ChannelURLExternal":                   {Behavior: BehaviorLog, Message: "ChannelURLExternal is empty"},
				"ChannelData.ChannelURL":                           {Behavior: BehaviorCritical, Message: "ChannelURL is required"},
				"ChannelData.CountryCode":                          {Behavior: BehaviorUnavailable, Message: "CountryCode is empty"},
				"ChannelData.PublishedAt":                          {Behavior: BehaviorUnavailable, Message: "PublishedAt is zero"},

				// Post fields
				"PostLink":              {Behavior: BehaviorCritical, Message: "PostLink is required"},
				"ChannelID":             {Behavior: BehaviorCritical, Message: "ChannelID is required"},
				"PostUID":               {Behavior: BehaviorCritical, Message: "PostUID is required"},
				"URL":                   {Behavior: BehaviorCritical, Message: "URL is required"},
				"PublishedAt":           {Behavior: BehaviorCritical, Message: "PublishedAt is required"},
				"CreatedAt":             {Behavior: BehaviorLog, Message: "CreatedAt is zero"},
				"LanguageCode":          {Behavior: BehaviorUnavailable, Message: "LanguageCode is empty"},
				"Engagement":            {Behavior: BehaviorLog, Message: "Engagement is zero"},
				"ViewCount":             {Behavior: BehaviorLog, Message: "ViewCount is zero"},
				"LikeCount":             {Behavior: BehaviorUnavailable, Message: "LikeCount not directly available on Telegram"},
				"ShareCount":            {Behavior: BehaviorLog, Message: "ShareCount is zero"},
				"CommentCount":          {Behavior: BehaviorLog, Message: "CommentCount is zero"},
				"CrawlLabel":            {Behavior: BehaviorLog, Message: "CrawlLabel is empty"},
				"ListIDs":               {Behavior: BehaviorUnavailable, Message: "ListIDs is empty"},
				"ChannelName":           {Behavior: BehaviorLog, Message: "ChannelName is empty"},
				"SearchTerms":           {Behavior: BehaviorUnavailable, Message: "SearchTerms is empty"},
				"SearchTermIDs":         {Behavior: BehaviorUnavailable, Message: "SearchTermIDs is empty"},
				"ProjectIDs":            {Behavior: BehaviorUnavailable, Message: "ProjectIDs is empty"},
				"ExerciseIDs":           {Behavior: BehaviorUnavailable, Message: "ExerciseIDs is empty"},
				"LabelData":             {Behavior: BehaviorUnavailable, Message: "LabelData is empty"},
				"LabelsMetadata":        {Behavior: BehaviorUnavailable, Message: "LabelsMetadata is empty"},
				"ProjectLabeledPostIDs": {Behavior: BehaviorUnavailable, Message: "ProjectLabeledPostIDs is empty"},
				"LabelerIDs":            {Behavior: BehaviorUnavailable, Message: "LabelerIDs is empty"},
				"AllLabels":             {Behavior: BehaviorUnavailable, Message: "AllLabels is empty"},
				"LabelIDs":              {Behavior: BehaviorUnavailable, Message: "LabelIDs is empty"},
				"IsAd":                  {Behavior: BehaviorLog, Message: "IsAd is false"},
				"TranscriptText":        {Behavior: BehaviorUnavailable, Message: "TranscriptText is empty"},
				"ImageText":             {Behavior: BehaviorUnavailable, Message: "ImageText is empty"},
				"VideoLength":           {Behavior: BehaviorUnavailable, Message: "VideoLength is null"},
				"IsVerified":            {Behavior: BehaviorUnavailable, Message: "IsVerified is null"},
				// ChannelData might need to be added here
				"PlatformName":               {Behavior: BehaviorCritical, Message: "PlatformName is required"},
				"SharedID":                   {Behavior: BehaviorUnavailable, Message: "SharedID is null"},
				"QuotedID":                   {Behavior: BehaviorUnavailable, Message: "QuotedID is null"},
				"RepliedID":                  {Behavior: BehaviorUnavailable, Message: "RepliedID is null"},
				"AILabel":                    {Behavior: BehaviorUnavailable, Message: "AILabel is null"},
				"RootPostID":                 {Behavior: BehaviorUnavailable, Message: "RootPostID is null"},
				"EngagementStepsCount":       {Behavior: BehaviorUnavailable, Message: "EngagementStepsCount is zero"},
				"OCRData":                    {Behavior: BehaviorUnavailable, Message: "OCRData is empty"},
				"PerformanceScores.Likes":    {Behavior: BehaviorUnavailable, Message: "PerformanceScores.Likes not directly available on Telegram"},
				"PerformanceScores.Shares":   {Behavior: BehaviorUnavailable, Message: "PerformanceScores.Shares is null"},
				"PerformanceScores.Comments": {Behavior: BehaviorUnavailable, Message: "PerformanceScores.Comments is null"},
				"PerformanceScores.Views":    {Behavior: BehaviorUnavailable, Message: "PerformanceScores.Views is zero"},
				"HasEmbedMedia":              {Behavior: BehaviorUnavailable, Message: "HasEmbedMedia is null"},
				"Description":                {Behavior: BehaviorLog, Message: "Description is empty"},
				"RepostChannelData":          {Behavior: BehaviorUnavailable, Message: "RepostChannelData is null"},
				"PostType":                   {Behavior: BehaviorLog, Message: "PostType is empty"},
				"InnerLink":                  {Behavior: BehaviorUnavailable, Message: "InnerLink is empty"},
				"PostTitle":                  {Behavior: BehaviorUnavailable, Message: "PostTitle is null"},
				"MediaData.DocumentName":     {Behavior: BehaviorUnavailable, Message: "MediaData.DocumentName is empty"},
				"IsReply":                    {Behavior: BehaviorUnavailable, Message: "IsReply is null"},
				"AdFields":                   {Behavior: BehaviorUnavailable, Message: "AdFields is null"},
				"LikesCount":                 {Behavior: BehaviorUnavailable, Message: "LikesCount not directly available on Telegram"},
				"SharesCount":                {Behavior: BehaviorLog, Message: "SharesCount is zero"},
				"CommentsCount":              {Behavior: BehaviorLog, Message: "CommentsCount is zero"},
				"ViewsCount":                 {Behavior: BehaviorLog, Message: "ViewsCount is zero"},
				"SearchableText":             {Behavior: BehaviorUnavailable, Message: "SearchableText is empty"},
				"AllText":                    {Behavior: BehaviorUnavailable, Message: "AllText is empty"},
				"ContrastAgentProjectIDs":    {Behavior: BehaviorUnavailable, Message: "ContrastAgentProjectIDs is empty"},
				"AgentIDs":                   {Behavior: BehaviorUnavailable, Message: "AgentIDs is empty"},
				"SegmentIDs":                 {Behavior: BehaviorUnavailable, Message: "SegmentIDs is empty"},
				"ThumbURL":                   {Behavior: BehaviorLog, Message: "ThumbURL is empty"},
				"MediaURL":                   {Behavior: BehaviorLog, Message: "MediaURL is empty"},
				"Comments":                   {Behavior: BehaviorLog, Message: "Comments is empty"},
				"Reactions":                  {Behavior: BehaviorLog, Message: "Reactions is empty"},
				"Outlinks":                   {Behavior: BehaviorLog, Message: "Outlinks is empty"},
				"CaptureTime":                {Behavior: BehaviorLog, Message: "CaptureTime is zero"},
				"Handle":                     {Behavior: BehaviorLog, Message: "Handle is empty"},
			},
		},
	}
}

// MergeConfigs merges user config with default config
func MergeConfigs(platform Platform, userConfig *UserValidationConfig) *ValidationConfig {
	defaults := DefaultConfigs()
	defaultConfig, ok := defaults[platform]
	if !ok {
		panic(fmt.Sprintf("no default config for platform: %s", platform))
	}

	// Create a deep copy of default rules
	mergedRules := make(map[string]FieldConfig)
	for k, v := range defaultConfig.Rules {
		mergedRules[k] = v
	}

	// Override with user-provided rules
	if userConfig != nil && userConfig.Rules != nil {
		for k, v := range userConfig.Rules {
			mergedRules[k] = v
		}
	}

	return &ValidationConfig{
		Platform: platform,
		Rules:    mergedRules,
	}
}

// LoadConfigFromJSON loads user config from JSON and merges with defaults
func LoadConfigFromJSON(jsonData []byte, platform Platform) (*ValidationConfig, error) {
	var userConfig UserValidationConfig
	if err := json.Unmarshal(jsonData, &userConfig); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	return MergeConfigs(platform, &userConfig), nil
}

// Validator handles field validation based on config
type Validator struct {
	config *ValidationConfig
	// logger *log.Logger
}

// NewValidator creates a new validator for a platform with default config
// func NewValidator(platform Platform, logger *log.Logger) *Validator {
func NewValidator(platform Platform) *Validator {
	// return NewValidatorWithConfig(MergeConfigs(platform, nil), logger)
	return NewValidatorWithConfig(MergeConfigs(platform, nil))
}

// NewValidatorWithUserConfig creates a validator with user-provided config merged with defaults
// func NewValidatorWithUserConfig(platform Platform, userConfig *UserValidationConfig, logger *log.Logger) *Validator {
func NewValidatorWithUserConfig(platform Platform, userConfig *UserValidationConfig) *Validator {
	// return NewValidatorWithConfig(MergeConfigs(platform, userConfig), logger)
	return NewValidatorWithConfig(MergeConfigs(platform, userConfig))
}

// NewValidatorWithConfig creates a validator with a complete config
// func NewValidatorWithConfig(config *ValidationConfig, logger *log.Logger) *Validator {
func NewValidatorWithConfig(config *ValidationConfig) *Validator {
	// if logger == nil {
	// 	logger = log.Default()
	// }

	return &Validator{
		config: config,
		// logger: logger,
	}
}

// ValidateChannelData validates a ChannelData struct
func (v *Validator) ValidateChannelData(data *model.ChannelData) *ValidationResult {
	result := &ValidationResult{
		Valid:           true,
		Errors:          []string{},
		Warnings:        []string{},
		UnavailableUsed: []string{},
		NullLogEvents:   []NullLogEvent{},
	}

	v.validateStruct("ChannelData", "channel", reflect.ValueOf(data).Elem(), result)

	return result
}

// ValidatePost validates a Post struct
func (v *Validator) ValidatePost(data *model.Post) *ValidationResult {
	result := &ValidationResult{
		Valid:           true,
		Errors:          []string{},
		Warnings:        []string{},
		UnavailableUsed: []string{},
		NullLogEvents:   []NullLogEvent{},
	}

	v.validateStruct("", "post", reflect.ValueOf(data).Elem(), result)

	return result
}

// validateStruct recursively validates struct fields
func (v *Validator) validateStruct(prefix string, dataType string, val reflect.Value, result *ValidationResult) {
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)
		fieldName := fieldType.Name

		fullPath := fieldName
		if prefix != "" {
			fullPath = prefix + "." + fieldName
		}

		// Handle nested structs (but not time.Time)
		if field.Kind() == reflect.Struct && fieldType.Type != reflect.TypeOf(time.Time{}) {
			v.validateStruct(fullPath, dataType, field, result)
			continue
		}

		// Handle pointer fields
		if field.Kind() == reflect.Ptr {
			if field.IsNil() {
				v.handleEmptyField(fullPath, dataType, result)
			}
			continue
		}

		// Handle slices and maps
		if field.Kind() == reflect.Slice || field.Kind() == reflect.Map {
			if field.IsNil() || field.Len() == 0 {
				v.handleEmptyField(fullPath, dataType, result)
			}
			continue
		}

		// Check if field is null/empty
		isEmpty := v.isEmptyValue(field)

		if isEmpty {
			v.handleEmptyField(fullPath, dataType, result)
		}
	}
}

// isEmptyValue checks if a field value is empty/null
func (v *Validator) isEmptyValue(field reflect.Value) bool {
	switch field.Kind() {
	case reflect.String:
		return field.String() == ""
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return field.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return field.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return field.Float() == 0
	case reflect.Bool:
		return !field.Bool()
	case reflect.Struct:
		// Special handling for time.Time
		if field.Type() == reflect.TypeOf(time.Time{}) {
			return field.Interface().(time.Time).IsZero()
		}
	}
	return false
}

// handleEmptyField processes an empty field based on config
func (v *Validator) handleEmptyField(fieldPath string, dataType string, result *ValidationResult) {
	config, exists := v.config.Rules[fieldPath]
	if !exists {
		// No rule defined, treat as optional
		return
	}

	isPlatformLimit := config.Behavior == BehaviorUnavailable

	// Create null log event
	nullEvent := NullLogEvent{
		Platform:        string(v.config.Platform),
		DataType:        dataType,
		FieldName:       fieldPath,
		StrategyUsed:    string(config.Behavior),
		IsPlatformLimit: isPlatformLimit,
		Message:         config.Message,
	}
	result.NullLogEvents = append(result.NullLogEvents, nullEvent)

	switch config.Behavior {
	case BehaviorCritical:
		result.Valid = false
		result.Errors = append(result.Errors, config.Message)
		log.Error().Str("platform", fmt.Sprint(v.config.Platform)).Str("data_type", dataType).Str("field_path", fieldPath).Msgf("null_validation: %s", config.Message)
	case BehaviorLog:
		result.Warnings = append(result.Warnings, config.Message)
		log.Info().Str("platform", fmt.Sprint(v.config.Platform)).Str("data_type", dataType).Str("field_path", fieldPath).Msgf("null_validation: %s", config.Message)

	case BehaviorUnavailable:
		result.UnavailableUsed = append(result.UnavailableUsed, fieldPath)
		log.Debug().Str("platform", fmt.Sprint(v.config.Platform)).Str("data_type", dataType).Str("field_path", fieldPath).Msgf("null_validation: %s", config.Message)

	case BehaviorOptional:
		// Still log the event, but no console output
	}
}

// func main() {
// 	// Example 1: Using default config
// 	fmt.Println("=== Example 1: Default Config ===")
// 	youtubeData := &ChannelData{
// 		ChannelID:   "UC123456",
// 		ChannelName: "Test Channel",
// 		ChannelURL:  "https://youtube.com/c/test",
// 	}

// 	validator := NewValidator(PlatformYouTube, nil)
// 	result := validator.ValidateChannelData(youtubeData)

// 	fmt.Printf("Valid: %v\n", result.Valid)
// 	fmt.Printf("Errors: %v\n", result.Errors)
// 	fmt.Printf("Warnings count: %d\n", len(result.Warnings))

// 	// Example 2: Using custom JSON config
// 	fmt.Println("\n=== Example 2: Custom JSON Config ===")
// 	customConfigJSON := `{
// 		"platform": "youtube",
// 		"rules": {
// 			"ChannelData.ChannelDescription": {
// 				"behavior": "critical",
// 				"message": "Description is now critical!"
// 			},
// 			"ChannelData.ChannelProfileImage": {
// 				"behavior": "optional",
// 				"message": "Profile image is now optional"
// 			}
// 		}
// 	}`

// 	config, err := LoadConfigFromJSON([]byte(customConfigJSON), PlatformYouTube)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	validator2 := NewValidatorWithConfig(config, nil)
// 	result2 := validator2.ValidateChannelData(youtubeData)

// 	fmt.Printf("Valid: %v\n", result2.Valid)
// 	fmt.Printf("Errors: %v\n", result2.Errors)

// 	// Example 3: Validating a Post
// 	fmt.Println("\n=== Example 3: Post Validation ===")
// 	post := &Post{
// 		PostLink:     "https://youtube.com/watch?v=abc",
// 		ChannelID:    "UC123456",
// 		PostUID:      "post_123",
// 		URL:          "https://youtube.com/watch?v=abc",
// 		PublishedAt:  time.Now(),
// 		PlatformName: "youtube",
// 		ChannelData: ChannelData{
// 			ChannelID:   "UC123456",
// 			ChannelName: "Test Channel",
// 			ChannelURL:  "https://youtube.com/c/test",
// 		},
// 	}

// 	validator3 := NewValidator(PlatformYouTube, nil)
// 	result3 := validator3.ValidatePost(post)

// 	fmt.Printf("Valid: %v\n", result3.Valid)
// 	fmt.Printf("Errors count: %d\n", len(result3.Errors))
// 	fmt.Printf("Warnings count: %d\n", len(result3.Warnings))
// 	fmt.Printf("Unavailable fields count: %d\n", len(result3.UnavailableUsed))
// 	fmt.Printf("Null log events count: %d\n", len(result3.NullLogEvents))

// 	// Example 4: Using default config without ChannelID
// 	fmt.Println("=== Example 1: Default Config ===")
// 	youtubeDataMissingID := &ChannelData{
// 		ChannelName: "Test Channel",
// 		ChannelURL:  "https://youtube.com/c/test",
// 	}

// 	result4 := validator.ValidateChannelData(youtubeDataMissingID)

// 	fmt.Printf("Valid: %v\n", result4.Valid)
// 	fmt.Printf("Errors: %v\n", result4.Errors)
// 	fmt.Printf("Warnings count: %d\n", len(result4.Warnings))

// 	// Print first few null events
// 	fmt.Println("\nSample Null Log Events:")
// 	for i, event := range result3.NullLogEvents {
// 		if i >= 3 {
// 			break
// 		}
// 		eventJSON, _ := json.MarshalIndent(event, "", "  ")
// 		fmt.Println(string(eventJSON))
// 	}
// }
