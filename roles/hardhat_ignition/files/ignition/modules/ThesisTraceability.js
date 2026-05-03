import { buildModule } from "@nomicfoundation/hardhat-ignition/modules";

export default buildModule("ThesisTraceabilityModule", (m) => {
  const traceability = m.contract("ThesisTraceability");

  return {
    traceability,
  };
});
