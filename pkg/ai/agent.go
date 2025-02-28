package ai

import (
	"context"
	"log"
	"time"
)

// AIAgent defines the interface for an AI analysis agent
type AIAgent interface {
	AnalyzeDiscrepancy(ctx context.Context, request AnalysisRequest) (*AnalysisResponse, error)
}

// AnalysisRequest contains data needed for AI analysis
type AnalysisRequest struct {
	Query           string
	DiscrepancyType string // "Performance", "ResultMismatch", "ErrorDivergence"
	PostgreSQL      BackendData
	YugabyteDB      BackendData
	AdditionalInfo  []string
}

// BackendData contains execution data for a single backend
type BackendData struct {
	ExecutionTime time.Duration
	Plan          string
	Error         string
	ResultSummary string
}

// AnalysisResponse contains the AI's analysis
type AnalysisResponse struct {
	Analysis       string
	Recommendation string
	Severity       string
}

// MockAIAgent is a placeholder implementation
type MockAIAgent struct{}

func NewMockAIAgent() *MockAIAgent {
	return &MockAIAgent{}
}

func (m *MockAIAgent) AnalyzeDiscrepancy(ctx context.Context, req AnalysisRequest) (*AnalysisResponse, error) {
	// In a real implementation, this would call an LLM API
	log.Printf("🤖 AI Agent analyzing %s discrepancy for query: %s", req.DiscrepancyType, req.Query)

	return &AnalysisResponse{
		Analysis:       "Mock analysis: YugabyteDB is slower likely due to missing index or network latency.",
		Recommendation: "Check indexes on the table used in the query.",
		Severity:       "Medium",
	}, nil
}
