package com.fs.starfarer.api.impl.campaign.rulecmd;

import java.util.List;
import java.util.Map;

import com.fs.starfarer.api.campaign.CampaignFleetAPI;
import com.fs.starfarer.api.campaign.InteractionDialogAPI;
import com.fs.starfarer.api.campaign.rules.MemoryAPI;
import com.fs.starfarer.api.impl.campaign.ids.MemFlags;
import com.fs.starfarer.api.util.Misc;
import com.fs.starfarer.api.util.Misc.Token;

public class MakeOtherFleetAllowDisengage extends BaseCommandPlugin {

	public boolean execute(String ruleId, InteractionDialogAPI dialog, List<Token> params, Map<String, MemoryAPI> memoryMap) {

		if (dialog == null) return false;
		if (!(dialog.getInteractionTarget() instanceof CampaignFleetAPI)) return false;
		
		String reason = "generic";
		boolean value;
		float expire = 0f;
		if (params.size() >= 2) {
			reason = params.get(0).getString(memoryMap);
			value = params.get(1).getBoolean(memoryMap);
			if (params.size() >= 3) {
				expire = params.get(2).getFloat(memoryMap);
			}
		} else if (params.size() < 1) {
			value = true;			
		} else {
			value = params.get(0).getBoolean(memoryMap);
		}
		
		MemoryAPI memory = dialog.getInteractionTarget().getMemoryWithoutUpdate();
		Misc.setFlagWithReason(memory, MemFlags.MEMORY_KEY_MAKE_ALLOW_DISENGAGE, reason, value, expire);

		return true;
	}

}
