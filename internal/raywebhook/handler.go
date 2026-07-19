package raywebhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/mrozacki/k8s-bridge/internal/ray"
	"github.com/mrozacki/k8s-bridge/internal/raybridge"
	"github.com/mrozacki/k8s-bridge/internal/rayconfig"
)

// webhookPath is where the manager serves this webhook; the
// MutatingWebhookConfiguration's clientConfig must point at the same path.
const webhookPath = "/mutate-ray-io-v1-rayjob"

// entrypointResourcesField is the RayJob spec field the pin is written into. It
// is a JSON-serialized string (a JSON object encoded as a string), per KubeRay.
var entrypointResourcesField = []string{"spec", "entrypointResources"}

// Handler is the controller-runtime admission handler that applies Decide to
// each incoming RayJob: it holds inner workloads (patch spec.suspend=true) and
// denies unsupported shapes.
type Handler struct {
	Cfg     *rayconfig.Config
	decoder admission.Decoder
}

// NewHandler builds a Handler with a decoder for the given scheme.
func NewHandler(cfg *rayconfig.Config, decoder admission.Decoder) *Handler {
	return &Handler{Cfg: cfg, decoder: decoder}
}

// Handle decodes the RayJob, applies the admission decision, and returns the
// response: a patch that suspends it, a denial, or an allow.
//
// Every admission DECISION is counted in ray_bridge_webhook_decisions_total
// (pinned/denied/skipped, see raybridge's metric file). Errored responses
// (decode/marshal failures) are deliberately NOT counted: they are server-side
// faults, not decisions about the RayJob, and controller-runtime's own webhook
// metrics already track response codes.
func (h *Handler) Handle(_ context.Context, req admission.Request) admission.Response {
	u := &unstructured.Unstructured{}
	if err := h.decoder.Decode(req, u); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	job, err := ray.FromUnstructured(u, ParseDefaults(h.Cfg))
	if err != nil {
		// A malformed worker-shape annotation is surfaced at submit time.
		raybridge.WebhookDecisionsTotal.WithLabelValues(raybridge.WebhookDecisionDenied).Inc()
		return admission.Denied(err.Error())
	}
	d := Decide(job, h.Cfg)
	if d.Deny {
		raybridge.WebhookDecisionsTotal.WithLabelValues(raybridge.WebhookDecisionDenied).Inc()
		return admission.Denied(d.Reason)
	}
	if !d.InjectPin {
		raybridge.WebhookDecisionsTotal.WithLabelValues(raybridge.WebhookDecisionSkipped).Inc()
		return admission.Allowed("")
	}
	// Merge the pin into any existing entrypointResources (a JSON-string field),
	// so a submitter's own resources are preserved and only the pin key is added.
	existing, _, _ := unstructured.NestedString(u.Object, entrypointResourcesField...)
	res := map[string]any{}
	if existing != "" {
		// DENY rather than silently overwrite: if the submitter's existing
		// entrypointResources is malformed JSON, merging the pin would drop
		// their whole map with no signal (review finding). Surface it so they
		// fix the typo instead of running with only the pin.
		if err := json.Unmarshal([]byte(existing), &res); err != nil {
			raybridge.WebhookDecisionsTotal.WithLabelValues(raybridge.WebhookDecisionDenied).Inc()
			return admission.Denied(fmt.Sprintf(
				"spec.entrypointResources is not valid JSON (%v); fix it so the ray-bridge pin can be merged in", err))
		}
	}
	res[d.PinName] = PinUnits
	merged, err := json.Marshal(res)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	patched := u.DeepCopy()
	_ = unstructured.SetNestedField(patched.Object, string(merged), entrypointResourcesField...)
	marshaled, err := json.Marshal(patched)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	raybridge.WebhookDecisionsTotal.WithLabelValues(raybridge.WebhookDecisionPinned).Inc()
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}

// SetupWithManager registers the webhook on the manager's webhook server.
// NOTE: the manager needs a serving certificate for its webhook server
// (cert-manager or a manually mounted cert); this is why ray-bridge does not
// wire the webhook by default. Call this only when certs are provisioned —
// see deploy/ray-bridge/webhook.yaml.
func (h *Handler) SetupWithManager(mgr ctrl.Manager) {
	mgr.GetWebhookServer().Register(webhookPath, &admission.Webhook{Handler: h})
}
