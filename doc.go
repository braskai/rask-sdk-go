// Package rask is the official Go client for the Rask AI API
// (https://docs.api.rask.ai).
//
// Create a client with your API client credentials; access tokens are
// fetched and refreshed automatically:
//
//	client, err := rask.NewClient(os.Getenv("RASK_CLIENT_ID"), os.Getenv("RASK_CLIENT_SECRET"))
//	if err != nil {
//		log.Fatal(err)
//	}
//	credits, err := client.GetCredits(ctx)
//
// Every API operation is a Client method named after its operationId in the
// API reference. Methods return the decoded response body, or an *APIError
// when the API answers with an error status.
package rask
