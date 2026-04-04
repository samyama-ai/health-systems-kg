// Health Systems Knowledge Graph — Schema
// 7 Node Labels, 6 Edge Types
// Sources: WHO SPAR, WHO NHWA, GAVI, Global Fund, IHME GHDx
// Focus: "What capacity exists to respond?"

// --- Indexes (create BEFORE loading data) ---
CREATE INDEX ON :Country(iso_code);
CREATE INDEX ON :Country(name);
CREATE INDEX ON :HealthFacility(id);
CREATE INDEX ON :HealthWorkforce(id);
CREATE INDEX ON :SupplyChain(id);
CREATE INDEX ON :Policy(id);
CREATE INDEX ON :FundingFlow(id);
CREATE INDEX ON :EmergencyResponse(id);

// --- Node Labels ---
// Country:            iso_code (ISO 3166-1 alpha-3), name, who_region, income_level
// HealthFacility:     id (HF-{iso}-{type}-{year}), name, type, country_code, year
// HealthWorkforce:    id (HW-{iso}-{profession}-{year}), profession, count, density_per_10k, country_code, year
// SupplyChain:        id (SC-{iso}-{vaccine}-{year}), vaccine_name, doses_shipped, doses_used, wastage_pct, country_code, year
// Policy:             id (PL-{iso}-{code}-{year}), name, category, score, country_code, year
// FundingFlow:        id (FF-{iso}-{source}-{component}-{year}), donor, amount_usd, disease_component, country_code, year
// EmergencyResponse:  id (ER-{iso}-{capacity}-{year}), capacity_code, capacity_name, score, country_code, year

// --- Edge Types ---
// LOCATED_IN:    HealthFacility -> Country
// SERVES:        HealthWorkforce -> Country
// SUPPLIES:      SupplyChain -> Country
// POLICY_OF:     Policy -> Country
// FUNDED_BY:     FundingFlow -> Country
// CAPACITY_FOR:  EmergencyResponse -> Country

// --- Cross-KG Bridge Properties ---
// Country.iso_code   -> surveillance-kg Country.iso_code
// Country.iso_code   -> health-determinants-kg Country.iso_code
// Country.iso_code   -> clinicaltrials-kg (via ICTRP country mapping)
